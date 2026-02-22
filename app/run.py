#!/usr/bin/env python
"""
Usage:
    python run.py                    # 기본 실행 (데이터 수집)
    python run.py collect            # 데이터 수집
    python run.py collect --market KOSPI --days 30
    python run.py scheduler          # 스케줄러 시작
    python run.py init-db            # DB 테이블 생성
"""

import sys
import argparse
from datetime import datetime, timedelta


def init_path():
    """PYTHONPATH 설정"""
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def cmd_collect(args):
    """데이터 수집"""
    from db import database
    from models import StockPrice, StockInfo
    from data_collector import DataPipeline
    from services import StockService

    database.create_tables()
    pipeline = DataPipeline()
    service = StockService(pipeline=pipeline)

    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')

    print(f"데이터 수집 시작: {start_date} ~ {end_date}")
    print(f"마켓: {args.market or '전체'}")
    print(f"섹터: {args.sector or '전체'}")
    print("-" * 50)

    result = pipeline.fetch(
        start_date=start_date,
        end_date=end_date,
        market=args.market,
        sector=args.sector,
        codes=args.codes.split(',') if args.codes else None
    )

    if result.data:
        result.db_saved_count = service.save_to_db(result.data, result.market)

    print("-" * 50)
    print(f"결과: {result.message}")
    print(f"성공: {result.success_count}/{result.total_codes}")
    print(f"DB 저장: {result.db_saved_count}건")
    print(f"소요 시간: {result.total_elapsed:.2f}초")


def cmd_scheduler(args):
    """스케줄러 시작"""
    from data_collector import DataScheduler, SCHEDULER_AVAILABLE

    if not SCHEDULER_AVAILABLE:
        print("apscheduler가 설치되어 있지 않습니다.")
        print("pip install apscheduler")
        return

    scheduler = DataScheduler.get_instance()

    # DB에 등록된 스케줄 로드
    loaded = scheduler.load_jobs_from_db()

    if not scheduler.get_jobs():
        print("등록된 스케줄이 없습니다. 어드민에서 추가해주세요.")

    print(f"\n스케줄러 시작 (등록된 작업: {len(scheduler.get_jobs())}개)")
    for job in scheduler.get_jobs():
        print(f"  - {job['id']}: 다음 실행 {job['next_run']}")
    print("-" * 50)
    print("Ctrl+C로 종료")

    scheduler.start()

    # 메인 스레드 유지
    try:
        while True:
            import time
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.stop()
        print("\n스케줄러 종료")


def cmd_init_db(args):
    """DB 테이블 생성"""
    from db import database
    # 모델 import 해야 ModelBase에 등록됨
    from models import StockPrice, StockInfo

    print("DB 테이블 생성 중...")
    database.create_tables()
    print("완료!")


def main():
    init_path()

    parser = argparse.ArgumentParser(description="퀀트 플랫폼")
    subparsers = parser.add_subparsers(dest="command", help="명령어")

    # collect 명령
    collect_parser = subparsers.add_parser("collect", help="데이터 수집")
    collect_parser.add_argument("--market", type=str, help="마켓 (KOSPI, KOSDAQ, S&P500 등)")
    collect_parser.add_argument("--sector", type=str, help="섹터")
    collect_parser.add_argument("--codes", type=str, help="종목 코드 (쉼표 구분)")
    collect_parser.add_argument("--days", type=int, default=30, help="수집 기간 (일)")

    # scheduler 명령
    subparsers.add_parser("scheduler", help="스케줄러 시작")

    # init-db 명령
    subparsers.add_parser("init-db", help="DB 테이블 생성")

    args = parser.parse_args()

    if args.command == "collect":
        cmd_collect(args)
    elif args.command == "scheduler":
        cmd_scheduler(args)
    elif args.command == "init-db":
        cmd_init_db(args)
    else:
        # 기본: 도움말
        parser.print_help()


if __name__ == "__main__":
    main()