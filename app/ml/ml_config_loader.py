"""
ML 설정 로더 (YAML 싱글톤 캐시)
"""
import importlib

import yaml
from pathlib import Path

_config = None


def get_ml_config() -> dict:
    """ml_config.yaml 로드 (싱글톤)"""
    global _config
    if _config is None:
        path = Path(__file__).parent / "ml_config.yaml"
        with open(path, "r", encoding="utf-8") as f:
            _config = yaml.safe_load(f)
    return _config


def get_algorithm_config(algorithm: str) -> dict:
    """알고리즘별 전체 설정 반환"""
    return get_ml_config()["algorithms"][algorithm]


def get_algorithm_defaults(algorithm: str) -> dict:
    """알고리즘 기본 파라미터 반환"""
    return get_ml_config()["algorithms"][algorithm]["defaults"]


def get_search_space(algorithm: str) -> dict:
    """Optuna 탐색 공간 반환"""
    return get_ml_config()["algorithms"][algorithm]["search_space"]


def get_classifier_class(algorithm: str):
    """알고리즘 분류기 클래스 동적 임포트"""
    config = get_algorithm_config(algorithm)
    module_path, class_name = config["classifier"].rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
