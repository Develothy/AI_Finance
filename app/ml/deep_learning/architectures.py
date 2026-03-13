"""
딥러닝 모델 아키텍처
====================

LSTMClassifier: 양방향 LSTM + FC head
TransformerClassifier: Positional Encoding + Transformer Encoder + FC head
"""

import math

import torch
import torch.nn as nn


class LSTMClassifier(nn.Module):
    """
    양방향 LSTM 분류기.

    Architecture:
        Input (batch, seq_len, n_features)
        → LSTM (bidirectional, num_layers)
        → 마지막 타임스텝 hidden
        → Dropout → FC → ReLU → Dropout → FC(2)
    """

    def __init__(
        self,
        n_features: int,
        hidden_size: int = 128,
        num_layers: int = 2,
        dropout: float = 0.3,
        bidirectional: bool = True,
    ):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
            bidirectional=bidirectional,
        )
        fc_in = hidden_size * (2 if bidirectional else 1)
        self.classifier = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(fc_in, hidden_size),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size, 2),
        )

    def forward(self, x):
        # x: (batch, seq_len, n_features)
        lstm_out, _ = self.lstm(x)       # (batch, seq_len, hidden*2)
        last_hidden = lstm_out[:, -1, :] # (batch, hidden*2)
        return self.classifier(last_hidden)  # (batch, 2)


class PositionalEncoding(nn.Module):
    """사인/코사인 위치 인코딩."""

    def __init__(self, d_model: int, max_len: int = 500, dropout: float = 0.1):
        super().__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model)
        )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0)  # (1, max_len, d_model)
        self.register_buffer("pe", pe)

    def forward(self, x):
        # x: (batch, seq_len, d_model)
        x = x + self.pe[:, :x.size(1), :]
        return self.dropout(x)


class TransformerClassifier(nn.Module):
    """
    Transformer Encoder 분류기.

    Architecture:
        Input (batch, seq_len, n_features)
        → Linear projection → d_model
        → PositionalEncoding
        → TransformerEncoder (num_layers, nhead)
        → Mean pooling
        → LayerNorm → FC → GELU → Dropout → FC(2)
    """

    def __init__(
        self,
        n_features: int,
        d_model: int = 128,
        nhead: int = 8,
        num_layers: int = 3,
        dim_feedforward: int = 256,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.input_proj = nn.Linear(n_features, d_model)
        self.pos_encoder = PositionalEncoding(d_model, dropout=dropout)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=d_model,
            nhead=nhead,
            dim_feedforward=dim_feedforward,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
        )
        self.transformer_encoder = nn.TransformerEncoder(
            encoder_layer, num_layers=num_layers
        )
        self.classifier = nn.Sequential(
            nn.LayerNorm(d_model),
            nn.Linear(d_model, d_model // 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(d_model // 2, 2),
        )

    def forward(self, x):
        # x: (batch, seq_len, n_features)
        x = self.input_proj(x)           # (batch, seq_len, d_model)
        x = self.pos_encoder(x)          # (batch, seq_len, d_model)
        x = self.transformer_encoder(x)  # (batch, seq_len, d_model)
        x = x.mean(dim=1)               # (batch, d_model) — mean pooling
        return self.classifier(x)        # (batch, 2)
