"""
Optuna DL 하이퍼파라미터 튜닝
==============================

아키텍처 + 학습 파라미터 탐색.
튜닝 시 축소 epoch(30)으로 빠른 trial 평가.
"""

import numpy as np
import optuna
import torch
import torch.nn as nn
from sklearn.metrics import f1_score as sklearn_f1_score
from torch.utils.data import DataLoader

from core import get_logger

from .dataset import SequenceDataset, create_sequences_from_df
from .architectures import LSTMClassifier, TransformerClassifier

logger = get_logger("dl_tuner")
optuna.logging.set_verbosity(optuna.logging.WARNING)

# 튜닝 시 축소 epoch
_TUNE_MAX_EPOCHS = 30


def _dl_objective(
    trial,
    algorithm: str,
    train_df,
    val_df,
    feature_columns: list[str],
    target_column: str,
    base_params: dict,
    device: torch.device,
):
    """단일 Optuna trial — DL 모델 학습 후 F1 반환."""

    # 공통 탐색
    seq_len = trial.suggest_int("seq_len", 10, 40, step=5)
    lr = trial.suggest_float("lr", 1e-4, 1e-2, log=True)
    batch_size = trial.suggest_categorical("batch_size", [32, 64, 128])
    dropout = trial.suggest_float("dropout", 0.1, 0.5)

    params = base_params.copy()
    params.update({
        "seq_len": seq_len,
        "lr": lr,
        "batch_size": batch_size,
        "dropout": dropout,
    })

    # 아키텍처별 탐색
    if algorithm == "lstm":
        params["hidden_size"] = trial.suggest_categorical("hidden_size", [64, 128, 256])
        params["num_layers"] = trial.suggest_int("num_layers", 1, 3)
    elif algorithm == "transformer":
        params["d_model"] = trial.suggest_categorical("d_model", [64, 128, 256])
        params["nhead"] = trial.suggest_categorical("nhead", [4, 8])
        params["num_layers"] = trial.suggest_int("num_layers", 2, 6)
        params["dim_feedforward"] = trial.suggest_categorical("dim_feedforward", [128, 256, 512])
        # d_model이 nhead로 나누어지지 않으면 prune
        if params["d_model"] % params["nhead"] != 0:
            raise optuna.TrialPruned()

    # 시퀀스 생성
    try:
        train_seqs, train_tgts = create_sequences_from_df(
            train_df, feature_columns, target_column, seq_len,
        )
        val_seqs, val_tgts = create_sequences_from_df(
            val_df, feature_columns, target_column, seq_len,
        )
    except ValueError:
        raise optuna.TrialPruned()

    train_loader = DataLoader(
        SequenceDataset(train_seqs, train_tgts),
        batch_size=batch_size,
        shuffle=True,
    )
    val_loader = DataLoader(
        SequenceDataset(val_seqs, val_tgts),
        batch_size=batch_size,
        shuffle=False,
    )

    # 모델 빌드
    n_features = len(feature_columns)
    if algorithm == "lstm":
        model = LSTMClassifier(
            n_features,
            params["hidden_size"],
            params["num_layers"],
            params["dropout"],
            params.get("bidirectional", True),
        ).to(device)
    else:
        model = TransformerClassifier(
            n_features,
            params["d_model"],
            params["nhead"],
            params["num_layers"],
            params["dim_feedforward"],
            params["dropout"],
        ).to(device)

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    criterion = nn.CrossEntropyLoss()

    # 축소 epoch으로 학습
    best_f1 = 0.0
    for epoch in range(_TUNE_MAX_EPOCHS):
        model.train()
        for X_b, y_b in train_loader:
            X_b, y_b = X_b.to(device), y_b.to(device)
            optimizer.zero_grad()
            loss = criterion(model(X_b), y_b)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()

        # 검증
        model.eval()
        all_preds, all_labels = [], []
        with torch.no_grad():
            for X_b, y_b in val_loader:
                X_b = X_b.to(device)
                preds = model(X_b).argmax(dim=1).cpu()
                all_preds.extend(preds.numpy())
                all_labels.extend(y_b.numpy())

        f1 = sklearn_f1_score(all_labels, all_preds, average="binary", zero_division=0)
        best_f1 = max(best_f1, f1)

        trial.report(f1, epoch)
        if trial.should_prune():
            raise optuna.TrialPruned()

    return best_f1


def tune_dl_hyperparameters(
    algorithm: str,
    train_df,
    val_df,
    feature_columns: list[str],
    target_column: str,
    base_params: dict,
    n_trials: int = 20,
    device: torch.device = None,
) -> dict:
    """
    Optuna DL 하이퍼파라미터 튜닝.

    Returns:
        {"best_params": dict, "best_value": float, "n_trials": int}
    """
    if device is None:
        device = torch.device("cpu")

    study = optuna.create_study(
        direction="maximize",
        pruner=optuna.pruners.MedianPruner(n_startup_trials=5, n_warmup_steps=10),
    )
    study.optimize(
        lambda trial: _dl_objective(
            trial, algorithm, train_df, val_df,
            feature_columns, target_column, base_params, device,
        ),
        n_trials=n_trials,
        show_progress_bar=False,
    )

    logger.info(
        f"DL 튜닝 완료: {algorithm} (best_f1={study.best_value:.4f}, trials={n_trials})",
        "tune_dl",
    )

    return {
        "best_params": study.best_params,
        "best_value": study.best_value,
        "n_trials": n_trials,
    }
