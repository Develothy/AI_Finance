"""
시퀀스 데이터셋
===============

FeatureStore의 플랫 행을 (batch, seq_len, n_features) 텐서로 변환.
종목(code)별 그룹 → 슬라이딩 윈도우 생성.
"""

import numpy as np
import torch
from torch.utils.data import Dataset

from core import get_logger

logger = get_logger("dl_dataset")


class SequenceDataset(Dataset):
    """PyTorch Dataset — (X_seq, y) 쌍 반환.

    X_seq: (seq_len, n_features) float32
    y:     scalar int64
    """

    def __init__(self, sequences: np.ndarray, targets: np.ndarray):
        self.X = torch.tensor(sequences, dtype=torch.float32)
        self.y = torch.tensor(targets, dtype=torch.long)

    def __len__(self):
        return len(self.y)

    def __getitem__(self, idx):
        return self.X[idx], self.y[idx]


def create_sequences_from_df(
    df,
    feature_columns: list[str],
    target_column: str,
    seq_len: int = 20,
) -> tuple[np.ndarray, np.ndarray]:
    """
    DataFrame을 종목별 슬라이딩 윈도우 시퀀스로 변환.

    각 종목(code)을 날짜순 정렬 후, seq_len 크기의 윈도우를 슬라이드.
    타겟은 윈도우 마지막 날(예측 시점)의 라벨.

    Args:
        df: DataFrame (code, date, features, target 컬럼 포함)
        feature_columns: 피처 컬럼명 리스트
        target_column: 타겟 컬럼명
        seq_len: 시퀀스 길이 (기본 20 거래일 ≈ 1개월)

    Returns:
        sequences: (N, seq_len, n_features) float32
        targets:   (N,) int64
    """
    all_sequences = []
    all_targets = []
    skipped_codes = 0

    for code, group in df.groupby("code"):
        group = group.sort_values("date")
        feat_vals = group[feature_columns].values  # (T, F)
        tgt_vals = group[target_column].values      # (T,)

        if len(group) < seq_len:
            skipped_codes += 1
            continue

        for i in range(seq_len, len(group)):
            seq = feat_vals[i - seq_len: i]  # (seq_len, F)
            tgt = tgt_vals[i]

            if np.isnan(tgt):
                continue

            all_sequences.append(seq)
            all_targets.append(int(tgt))

    if skipped_codes > 0:
        logger.info(
            f"시퀀스 미달 종목 skip: {skipped_codes}개 (seq_len={seq_len})",
            "create_sequences",
        )

    if not all_sequences:
        raise ValueError(f"유효한 시퀀스 없음 (seq_len={seq_len})")

    sequences = np.array(all_sequences, dtype=np.float32)
    targets = np.array(all_targets, dtype=np.int64)

    logger.info(
        f"시퀀스 생성 완료: {len(sequences)}개 (seq_len={seq_len}, features={len(feature_columns)})",
        "create_sequences",
    )

    return sequences, targets
