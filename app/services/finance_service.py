import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Lock
from typing import List, Optional, Tuple

import FinanceDataReader as fdr
from dateutil.relativedelta import relativedelta

from app.schemas.stock_schemas import (
    StockItem,
    StockDataResponse,
    MultipleStockDataResponse,
    StockDataPoint
)

logger = logging.getLogger(__name__)


class FinanceService:
    _executor = ThreadPoolExecutor(max_workers=10)
    _last_call_time = 0
    _lock = Lock()
    _min_interval = 0.1  # API 호출 간격 (초)

    @staticmethod
    def _rate_limit():
        # API 호출 속도 제한
        with FinanceService._lock:
            now = time.time()
            time_since_last = now - FinanceService._last_call_time
            if time_since_last < FinanceService._min_interval:
                time.sleep(FinanceService._min_interval - time_since_last)
            FinanceService._last_call_time = time.time()

    @staticmethod
    def get_stock_list(market: str = 'KRX') -> List[StockItem]:
        try:
            FinanceService._rate_limit()
            df_stocks = fdr.StockListing(market)

            stocks = []
            for _, row in df_stocks.iterrows():
                stock = StockItem(
                    symbol=row.get('Code', ''),
                    name=row.get('Name', ''),
                    market=market
                )
                stocks.append(stock)

            logger.info(f"Successfully fetched {len(stocks)} stocks from {market}")
            return stocks

        except Exception as e:
            logger.error(f"Error fetching stock list for {market}: {e}", exc_info=True)
            # TODO raise 적당한 예외 처리 필요

    @staticmethod
    def get_stock_data(
            symbol: str,
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> StockDataResponse:
        start_date, end_date = FinanceService._calculate_date_range(
            start_date, end_date, days
        )

        try:
            FinanceService._rate_limit()
            df = fdr.DataReader(symbol, start_date, end_date)

            if df.empty:
                logger.warning(f"No data found for {symbol}")
                return StockDataResponse(
                    symbol=symbol,
                    data=[],
                    start_date=start_date,
                    end_date=end_date,
                    total_count=0
                )

            stock_data = []
            for date, row in df.iterrows():
                try:
                    data_point = StockDataPoint(
                        date=date.strftime('%Y-%m-%d'),
                        open=float(row.get('Open') or 0),
                        high=float(row.get('High') or 0),
                        low=float(row.get('Low') or 0),
                        close=float(row.get('Close') or 0),
                        volume=int(row.get('Volume') or 0)
                    )
                    stock_data.append(data_point)
                except (ValueError, TypeError) as e:
                    logger.warning(f"Skipping invalid data point for {symbol} on {date}: {e}")
                    continue

            logger.info(f"Successfully fetched {len(stock_data)} data points for {symbol}")

            return StockDataResponse(
                symbol=symbol,
                name=None, # TODO 종목명 캐싱 등 조치 필요
                data=stock_data,
                start_date=start_date,
                end_date=end_date,
                total_count=len(stock_data)
            )

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}", exc_info=True)
            return StockDataResponse(
                symbol=symbol,
                data=[],
                start_date=start_date,
                end_date=end_date,
                total_count=0
            )

    @staticmethod
    async def get_multiple_stocks(
            symbols: List[str],
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> MultipleStockDataResponse:
        loop = asyncio.get_event_loop()

        # 비동기 태스크
        tasks = [
            loop.run_in_executor(
                FinanceService._executor,
                FinanceService.get_stock_data,
                symbol, start_date, end_date, days
            )
            for symbol in symbols
        ]

        # 예외 모두 포함된 결과임
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results = []
        failed_symbols = []

        for symbol, result in zip(symbols, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch data for {symbol}: {result}")
                failed_symbols.append(symbol)

                valid_results.append(StockDataResponse(
                    symbol=symbol,
                    data=[],
                    start_date=start_date or '',
                    end_date=end_date or '',
                    total_count=0
                ))
            elif isinstance(result, StockDataResponse):
                valid_results.append(result)
            else:
                logger.warning(f"Unexpected result type for {symbol}: {type(result)}")
                failed_symbols.append(symbol)

        if failed_symbols:
            logger.warning(f"Failed to fetch data for symbols: {failed_symbols}")

        logger.info(
            f"Fetched data for {len(valid_results)} symbols "
            f"({len(failed_symbols)} failed)"
        )

        return MultipleStockDataResponse(
            stocks_data=valid_results,
            request_count=len(symbols)
        )

    @staticmethod
    def _calculate_date_range(
            start_date: Optional[str] = None,
            end_date: Optional[str] = None,
            days: int = 30
    ) -> Tuple[str, str]:
        if not end_date:
            if start_date:
                end_dt = datetime.strptime(start_date, '%Y-%m-%d') + relativedelta(days=days-1)
                end_date = end_dt.strftime('%Y-%m-%d')
            else:
                end_dt = datetime.now()
                end_date = end_dt.strftime('%Y-%m-%d')
        else:
            try:
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                end_dt = datetime.now()
                end_date = end_dt.strftime('%Y-%m-%d')

        if not start_date:
            start_dt = end_dt - relativedelta(days=days-1)
            start_date = start_dt.strftime('%Y-%m-%d')

        return start_date, end_date


    @classmethod
    def shutdown(cls):
        cls._executor.shutdown(wait=True)
        logger.info("FinanceService executor shutdown complete")