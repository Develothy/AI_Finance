"""
DB 연결 관리
==========

Usage:
    from db import database, ModelBase

    # 세션 사용
    with database.session() as session:
        session.query(...)

    # 테이블 생성
    database.create_tables()
"""

from pathlib import Path
from contextlib import contextmanager

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base

from config import settings
from core import get_logger

logger = get_logger("database")

# 모델 정의용 Base
ModelBase = declarative_base()


class _Database:
    """DB 연결 관리 (싱글톤)"""

    def __init__(self):
        self._engine = None
        self._session_factory = None

    def _ensure_initialized(self):
        """지연 초기화"""
        if self._engine is not None:
            return

        # SQLite면 디렉토리 생성
        if settings.DB_TYPE == "sqlite":
            Path(settings.SQLITE_PATH).parent.mkdir(parents=True, exist_ok=True)
            self._engine = create_engine(
                settings.db_url,
                connect_args={"check_same_thread": False},
                echo=settings.DEBUG
            )
        else:
            self._engine = create_engine(
                settings.db_url,
                pool_size=5,
                max_overflow=10,
                echo=settings.DEBUG
            )

        self._session_factory = sessionmaker(bind=self._engine)

    @contextmanager
    def session(self):
        """세션 컨텍스트 매니저"""
        self._ensure_initialized()
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def create_tables(self):
        """테이블 생성 + 누락 컬럼 자동 추가 + 불필요 컬럼 삭제"""
        self._ensure_initialized()
        ModelBase.metadata.create_all(self._engine)
        self._migrate_missing_columns()
        self._migrate_drop_obsolete_columns()

    def _migrate_missing_columns(self):
        """모델에 정의된 컬럼 중 DB에 없는 컬럼을 ALTER TABLE로 추가"""
        insp = inspect(self._engine)

        for table_name, table in ModelBase.metadata.tables.items():
            if not insp.has_table(table_name):
                continue

            existing_cols = {c["name"] for c in insp.get_columns(table_name)}
            for col in table.columns:
                if col.name in existing_cols:
                    continue

                col_type = col.type.compile(dialect=self._engine.dialect)
                nullable = "NULL" if col.nullable else "NOT NULL"
                sql = f'ALTER TABLE {table_name} ADD COLUMN {col.name} {col_type} {nullable}'

                try:
                    with self._engine.begin() as conn:
                        conn.execute(text(sql))
                    logger.info(
                        f"컬럼 추가: {table_name}.{col.name} ({col_type})",
                        "_migrate_missing_columns",
                    )
                except Exception as e:
                    logger.warning(
                        f"컬럼 추가 실패: {table_name}.{col.name} - {e}",
                        "_migrate_missing_columns",
                    )

    # feature_store 정규화로 제거된 컬럼 (거시지표 + 시장센티먼트)
    _OBSOLETE_COLUMNS = {
        "feature_store": [
            "krw_usd", "vix", "kospi_index", "us_10y", "kr_3y",
            "sp500", "wti", "gold", "fed_rate", "usd_index", "us_cpi",
            "market_sentiment", "market_news_volume",
        ],
    }

    def _migrate_drop_obsolete_columns(self):
        """모델에서 제거된 컬럼을 DB에서도 DROP"""
        insp = inspect(self._engine)

        for table_name, columns in self._OBSOLETE_COLUMNS.items():
            if not insp.has_table(table_name):
                continue

            existing_cols = {c["name"] for c in insp.get_columns(table_name)}
            model_cols = {
                c.name for c in ModelBase.metadata.tables[table_name].columns
            } if table_name in ModelBase.metadata.tables else set()

            for col_name in columns:
                if col_name in existing_cols and col_name not in model_cols:
                    try:
                        sql = f'ALTER TABLE {table_name} DROP COLUMN {col_name}'
                        with self._engine.begin() as conn:
                            conn.execute(text(sql))
                        logger.info(
                            f"컬럼 삭제: {table_name}.{col_name}",
                            "_migrate_drop",
                        )
                    except Exception as e:
                        logger.warning(
                            f"컬럼 삭제 실패: {table_name}.{col_name} - {e}",
                            "_migrate_drop",
                        )

    def drop_tables(self):
        """테이블 삭제"""
        self._ensure_initialized()
        ModelBase.metadata.drop_all(self._engine)

    @property
    def engine(self):
        self._ensure_initialized()
        return self._engine


# DB 인스턴스 (싱글톤)
database = _Database()