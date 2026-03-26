"""
딥러닝 학습 파이프라인
======================

FeatureStore 데이터 로드 → 시퀀스 생성 → LSTM/Transformer 학습
→ Early stopping + Optuna 튜닝 → .pt 저장 → DB 저장
"""

import json
import os
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.preprocessing import RobustScaler
from torch.utils.data import DataLoader

from config import settings
from core import get_logger
from db import database
from models import MLModel, MLTrainingLog
from repositories import MLRepository

from ..feature_engineer import PHASE7_FEATURE_COLUMNS
from .dataset import SequenceDataset, create_sequences_from_df
from .architectures import LSTMClassifier, TransformerClassifier

logger = get_logger("dl_trainer")

# 저장 디렉토리
SAVED_MODELS_DIR = Path(
    os.environ.get(
        "MODEL_SAVE_DIR",
        Path(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))) / "saved_models",
    )
)

# 기본 하이퍼파라미터
DL_DEFAULTS = {
    "lstm": {
        "seq_len": 20,
        "hidden_size": 128,
        "num_layers": 2,
        "dropout": 0.3,
        "bidirectional": True,
        "lr": 1e-3,
        "batch_size": 64,
        "epochs": 100,
        "patience": 10,
        "grad_clip": 1.0,
    },
    "transformer": {
        "seq_len": 20,
        "d_model": 128,
        "nhead": 8,
        "num_layers": 3,
        "dim_feedforward": 256,
        "dropout": 0.2,
        "lr": 5e-4,
        "batch_size": 64,
        "epochs": 100,
        "patience": 10,
        "grad_clip": 1.0,
    },
}


def _get_device() -> torch.device:
    """사용 가능한 디바이스 자동 감지."""
    if torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


class DeepLearningTrainer:
    """딥러닝 모델 학습기 (LSTM / Transformer)."""

    def __init__(self):
        SAVED_MODELS_DIR.mkdir(parents=True, exist_ok=True)
        self.device = _get_device()
        logger.info(f"DL Trainer 초기화 (device={self.device})", "__init__")

    def train(
        self,
        market: str,
        algorithm: str,
        target_column: str = "target_class_1d",
        train_ratio: float = 0.85,
        val_ratio: float = 0.15,
        optuna_trials: int = 20,
        feature_columns: list[str] = None,
        dl_params: dict = None,
    ) -> dict:
        """
        DL 모델 학습 실행.

        ModelTrainer.train()과 동일한 시그니처·반환값 유지.

        Returns:
            {"model_id": int, "model_name": str, "metrics": dict, "feature_importance": dict}
        """
        features = feature_columns or PHASE7_FEATURE_COLUMNS
        model_type = "classification"

        # 기본 파라미터 병합
        params = DL_DEFAULTS[algorithm].copy()
        if dl_params:
            params.update(dl_params)

        # 1. 학습 이력 생성
        training_log_id = self._create_training_log(
            algorithm=algorithm,
            model_type=model_type,
            market=market,
            target_column=target_column,
        )

        try:
            # 2. 데이터 로드 (code 컬럼 포함)
            df = self._load_data(market, features, target_column)
            if len(df) < 200:
                raise ValueError(f"DL 학습 데이터 부족: {len(df)}행 (최소 200행)")

            # 2.5 NaN 비율 높은 피처 제외
            features, dropped = self._filter_features_by_nan(df, features)
            if not features:
                raise ValueError("사용 가능한 피처 없음 (모든 피처 NaN 비율 초과)")

            # 3. 시계열 분할
            train_df, val_df, _ = self._split_data(df, train_ratio, val_ratio)

            # 4. NaN imputation (DL은 NaN 불가)
            imputer = SimpleImputer(strategy="median")
            train_df = train_df.copy()
            val_df = val_df.copy()
            train_df[features] = imputer.fit_transform(train_df[features])
            val_df[features] = imputer.transform(val_df[features])

            # 5. 스케일링
            scaler = RobustScaler()
            train_df[features] = scaler.fit_transform(train_df[features])
            val_df[features] = scaler.transform(val_df[features])

            # 6. Optuna 튜닝 (선택)
            best_params = {}
            tune_result = None
            if optuna_trials > 0:
                from .dl_tuner import tune_dl_hyperparameters

                tune_result = tune_dl_hyperparameters(
                    algorithm=algorithm,
                    train_df=train_df,
                    val_df=val_df,
                    feature_columns=features,
                    target_column=target_column,
                    base_params=params,
                    n_trials=optuna_trials,
                    device=self.device,
                )
                best_params = tune_result["best_params"]
                params.update(best_params)

            seq_len = params["seq_len"]

            # 7. 시퀀스 생성
            train_seqs, train_tgts = create_sequences_from_df(
                train_df, features, target_column, seq_len=seq_len,
            )
            val_seqs, val_tgts = create_sequences_from_df(
                val_df, features, target_column, seq_len=seq_len,
            )

            train_loader = DataLoader(
                SequenceDataset(train_seqs, train_tgts),
                batch_size=params["batch_size"],
                shuffle=True,
                drop_last=False,
            )
            val_loader = DataLoader(
                SequenceDataset(val_seqs, val_tgts),
                batch_size=params["batch_size"],
                shuffle=False,
            )

            # 8. 모델 빌드
            n_features = len(features)
            model = self._build_model(algorithm, n_features, params)
            model = model.to(self.device)

            logger.info(
                f"모델 빌드 완료: {algorithm} (params={n_features}F, "
                f"train_seqs={len(train_seqs)}, val_seqs={len(val_seqs)})",
                "train",
            )

            # 9. 학습
            trained_model, train_info = self._train_loop(model, train_loader, val_loader, params)

            # 10. 최종 평가
            metrics = self._evaluate(trained_model, val_loader)

            # 11. 모델 저장 (.pt)
            model_name = f"{algorithm[:4]}_class_{market.lower()}_{target_column.replace('target_class_', '')}d"
            version = self._get_next_version(model_name)
            file_name = f"{model_name}_v{version}.pt"
            model_path = str(SAVED_MODELS_DIR / file_name)

            save_data = {
                "model_state_dict": trained_model.cpu().state_dict(),
                "model_class": algorithm,
                "model_params": params,
                "scaler": scaler,
                "imputer": imputer,
                "feature_columns": features,
                "n_features": n_features,
                "algorithm": algorithm,
                "target_column": target_column,
                "seq_len": seq_len,
            }
            torch.save(save_data, model_path)

            # 12. DB 저장
            model_id = self._save_model_to_db(
                model_name=model_name,
                model_type=model_type,
                algorithm=algorithm,
                market=market,
                target_column=target_column,
                hyperparameters=params,
                feature_columns=features,
                train_df=train_df,
                metrics=metrics,
                model_path=model_path,
                version=version,
            )

            # 13. 학습 이력 업데이트
            self._update_training_log(
                log_id=training_log_id,
                model_id=model_id,
                status="success",
                train_df=train_df,
                val_df=val_df,
                feature_count=len(features),
                metrics=metrics,
                feature_importance={},
                hyperparameters=params,
                optuna_trials=optuna_trials if optuna_trials > 0 else None,
                best_trial_value=tune_result["best_value"] if tune_result else None,
            )

            logger.info(
                f"DL 학습 완료: {model_name} v{version} "
                f"(f1={metrics['f1']}, acc={metrics['accuracy']})",
                "train",
            )

            return {
                "model_id": model_id,
                "model_name": f"{model_name}_v{version}",
                "metrics": metrics,
                "feature_importance": {},
            }

        except Exception as e:
            self._update_training_log(
                log_id=training_log_id,
                status="failed",
                error_message=str(e)[:1000],
            )
            logger.error(f"DL 학습 실패: {e}", "train")
            raise

    # ------------------------------------------------------------------
    # 모델 빌드
    # ------------------------------------------------------------------

    def _build_model(self, algorithm: str, n_features: int, params: dict) -> nn.Module:
        """알고리즘별 모델 생성."""
        if algorithm == "lstm":
            return LSTMClassifier(
                n_features=n_features,
                hidden_size=params["hidden_size"],
                num_layers=params["num_layers"],
                dropout=params["dropout"],
                bidirectional=params.get("bidirectional", True),
            )
        elif algorithm == "transformer":
            return TransformerClassifier(
                n_features=n_features,
                d_model=params["d_model"],
                nhead=params["nhead"],
                num_layers=params["num_layers"],
                dim_feedforward=params["dim_feedforward"],
                dropout=params["dropout"],
            )
        raise ValueError(f"지원하지 않는 DL 알고리즘: {algorithm}")

    # ------------------------------------------------------------------
    # 학습 루프
    # ------------------------------------------------------------------

    def _train_loop(
        self,
        model: nn.Module,
        train_loader: DataLoader,
        val_loader: DataLoader,
        params: dict,
    ) -> tuple[nn.Module, dict]:
        """학습 루프 — Early stopping, gradient clipping, mixed precision."""
        optimizer = torch.optim.AdamW(
            model.parameters(), lr=params["lr"], weight_decay=1e-4,
        )
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
            optimizer, mode="max", factor=0.5, patience=5,
        )
        criterion = nn.CrossEntropyLoss()

        use_amp = self.device.type == "cuda"
        amp_scaler = torch.amp.GradScaler() if use_amp else None

        best_f1 = 0.0
        best_state = None
        patience_counter = 0
        epochs = params["epochs"]
        patience = params["patience"]
        grad_clip = params["grad_clip"]

        for epoch in range(epochs):
            # --- Train ---
            model.train()
            total_loss = 0.0

            for X_batch, y_batch in train_loader:
                X_batch = X_batch.to(self.device)
                y_batch = y_batch.to(self.device)
                optimizer.zero_grad()

                if use_amp:
                    with torch.amp.autocast(device_type="cuda"):
                        logits = model(X_batch)
                        loss = criterion(logits, y_batch)
                    amp_scaler.scale(loss).backward()
                    amp_scaler.unscale_(optimizer)
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                    amp_scaler.step(optimizer)
                    amp_scaler.update()
                else:
                    logits = model(X_batch)
                    loss = criterion(logits, y_batch)
                    loss.backward()
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                    optimizer.step()

                total_loss += loss.item()

            # --- Validate ---
            val_metrics = self._evaluate(model, val_loader)
            val_f1 = val_metrics["f1"]
            scheduler.step(val_f1)

            if val_f1 > best_f1:
                best_f1 = val_f1
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
                patience_counter = 0
            else:
                patience_counter += 1

            if (epoch + 1) % 10 == 0:
                avg_loss = total_loss / max(len(train_loader), 1)
                logger.info(
                    f"Epoch {epoch + 1}/{epochs}: loss={avg_loss:.4f}, "
                    f"val_f1={val_f1:.4f}, best_f1={best_f1:.4f}",
                    "_train_loop",
                )

            if patience_counter >= patience:
                logger.info(
                    f"Early stopping at epoch {epoch + 1} (best_f1={best_f1:.4f})",
                    "_train_loop",
                )
                break

        # 최적 모델 복원
        if best_state:
            model.load_state_dict(best_state)
            model = model.to(self.device)

        return model, {"best_f1": best_f1, "epochs_trained": epoch + 1}

    # ------------------------------------------------------------------
    # 평가
    # ------------------------------------------------------------------

    @torch.no_grad()
    def _evaluate(self, model: nn.Module, loader: DataLoader) -> dict:
        """모델 평가 — accuracy, precision, recall, f1, auc_roc."""
        model.eval()
        all_preds = []
        all_labels = []
        all_probs = []

        for X_batch, y_batch in loader:
            X_batch = X_batch.to(self.device)
            logits = model(X_batch)
            probs = torch.softmax(logits, dim=1)
            preds = logits.argmax(dim=1)

            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(y_batch.numpy())
            all_probs.extend(probs[:, 1].cpu().numpy())

        y_pred = np.array(all_preds)
        y_true = np.array(all_labels)
        y_proba = np.array(all_probs)

        metrics = {
            "accuracy": round(float(accuracy_score(y_true, y_pred)), 4),
            "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
            "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
            "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        }
        try:
            metrics["auc_roc"] = round(float(roc_auc_score(y_true, y_proba)), 4)
        except ValueError:
            metrics["auc_roc"] = None

        return metrics

    # ------------------------------------------------------------------
    # 데이터 로드 / 분할
    # ------------------------------------------------------------------

    def _load_data(
        self, market: str, features: list[str], target_column: str,
    ) -> pd.DataFrame:
        """feature_store + 외부 소스에서 학습 데이터 로드 (code 컬럼 포함)."""
        from ml.feature_loader import EXTERNAL_FEATURE_NAMES, merge_external_features

        with database.session() as session:
            repo = MLRepository(session)
            rows = repo.get_features_by_market(market)

            if not rows:
                raise ValueError(f"피처 데이터 없음: {market}")

            fs_features = [c for c in features if c not in EXTERNAL_FEATURE_NAMES]

            data = []
            for r in rows:
                row_dict = {"date": r.date, "code": r.code}
                for col in fs_features:
                    val = getattr(r, col, None)
                    row_dict[col] = float(val) if val is not None else None
                target_val = getattr(r, target_column, None)
                row_dict[target_column] = int(target_val) if target_val is not None else None
                data.append(row_dict)

            df = pd.DataFrame(data)
            df = df.dropna(subset=[target_column])

            # 외부 피처 병합 (거시지표 + 시장센티먼트)
            df = merge_external_features(df, session, market, features)

            nan_counts = df[features].isna().sum()
            nan_features = nan_counts[nan_counts > 0]
            if len(nan_features) > 0:
                logger.info(
                    f"NaN 피처 {len(nan_features)}개 (imputer로 처리 예정)",
                    "_load_data",
                )
            return df

    def _filter_features_by_nan(
        self,
        df: pd.DataFrame,
        features: list[str],
        threshold: float = 0.5,
    ) -> tuple[list[str], list[str]]:
        """NaN 비율 threshold 이상인 피처 제외."""
        n_rows = len(df)
        usable = []
        dropped = []

        for col in features:
            nan_ratio = df[col].isna().sum() / n_rows
            if nan_ratio >= threshold:
                dropped.append(col)
            else:
                usable.append(col)

        if dropped:
            logger.info(
                f"NaN 비율 초과로 피처 제외 ({len(dropped)}개): {dropped}",
                "_filter_features",
            )

        return usable, dropped

    def _split_data(
        self,
        df: pd.DataFrame,
        train_ratio: float,
        val_ratio: float,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """시계열 기반 데이터 분할."""
        n = len(df)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_df = df.iloc[:train_end].copy()
        val_df = df.iloc[train_end:val_end].copy()
        test_df = df.iloc[val_end:].copy()

        logger.info(
            f"데이터 분할: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}",
            "_split_data",
        )
        return train_df, val_df, test_df

    # ------------------------------------------------------------------
    # DB 헬퍼
    # ------------------------------------------------------------------

    def _get_next_version(self, model_name: str) -> int:
        """다음 버전 번호."""
        with database.session() as session:
            repo = MLRepository(session)
            existing = repo.get_model_by_name(model_name)
            if existing:
                return existing.version + 1
            return 1

    def _save_model_to_db(self, **kwargs) -> int:
        """모델 메타데이터 DB 저장."""
        with database.session() as session:
            repo = MLRepository(session)

            repo.deactivate_models(
                market=kwargs["market"],
                model_type=kwargs["model_type"],
                target_column=kwargs["target_column"],
                algorithm=kwargs["algorithm"],
            )

            metrics = kwargs["metrics"]
            # hyperparameters에서 JSON 직렬화 불가능한 값 제거
            hp = {}
            for k, v in kwargs["hyperparameters"].items():
                if isinstance(v, (int, float, str, bool, type(None))):
                    hp[k] = v

            model = repo.save_model({
                "model_name": kwargs["model_name"],
                "model_type": kwargs["model_type"],
                "algorithm": kwargs["algorithm"],
                "market": kwargs["market"],
                "target_column": kwargs["target_column"],
                "hyperparameters": json.dumps(hp),
                "feature_columns": json.dumps(kwargs["feature_columns"]),
                "train_start_date": kwargs["train_df"]["date"].min(),
                "train_end_date": kwargs["train_df"]["date"].max(),
                "train_sample_count": len(kwargs["train_df"]),
                "accuracy": metrics.get("accuracy"),
                "precision_score": metrics.get("precision"),
                "recall": metrics.get("recall"),
                "f1_score": metrics.get("f1"),
                "auc_roc": metrics.get("auc_roc"),
                "model_path": kwargs["model_path"],
                "is_active": True,
                "version": kwargs["version"],
            })
            return model.id

    def _create_training_log(self, **kwargs) -> int:
        """학습 이력 생성 (running 상태)."""
        with database.session() as session:
            repo = MLRepository(session)
            log = repo.save_training_log({
                "algorithm": kwargs["algorithm"],
                "model_type": kwargs["model_type"],
                "market": kwargs["market"],
                "target_column": kwargs["target_column"],
                "status": "running",
                "started_at": datetime.now(),
            })
            return log.id

    def _update_training_log(self, log_id: int, status: str = None, **kwargs):
        """학습 이력 업데이트."""
        with database.session() as session:
            repo = MLRepository(session)
            updates = {"finished_at": datetime.now()}

            if status:
                updates["status"] = status
            if kwargs.get("model_id"):
                updates["model_id"] = kwargs["model_id"]
            if kwargs.get("error_message"):
                updates["error_message"] = kwargs["error_message"]
            if kwargs.get("train_df") is not None:
                updates["train_start_date"] = kwargs["train_df"]["date"].min()
                updates["train_end_date"] = kwargs["train_df"]["date"].max()
                updates["train_samples"] = len(kwargs["train_df"])
            if kwargs.get("val_df") is not None:
                updates["val_start_date"] = kwargs["val_df"]["date"].min()
                updates["val_end_date"] = kwargs["val_df"]["date"].max()
                updates["val_samples"] = len(kwargs["val_df"])
            if kwargs.get("feature_count"):
                updates["feature_count"] = kwargs["feature_count"]
            if kwargs.get("metrics"):
                updates["metrics_json"] = json.dumps(kwargs["metrics"])
            if kwargs.get("feature_importance") is not None:
                updates["feature_importance_json"] = json.dumps(kwargs["feature_importance"])
            if kwargs.get("hyperparameters"):
                hp = {}
                for k, v in kwargs["hyperparameters"].items():
                    if isinstance(v, (int, float, str, bool, type(None))):
                        hp[k] = v
                updates["hyperparameters_json"] = json.dumps(hp)
            if kwargs.get("optuna_trials") is not None:
                updates["optuna_trials"] = kwargs["optuna_trials"]
            if kwargs.get("best_trial_value") is not None:
                updates["best_trial_value"] = kwargs["best_trial_value"]

            repo.update_training_log(log_id, updates)
