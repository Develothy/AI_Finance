import FinanceDataReader as fdr

from typing import List

from app.schemas.stock_schemas import StockItem


class FinanceService:
    @staticmethod
    def get_stock_list(market: str = 'KRX') -> List[StockItem]:
        df_stocks = fdr.StockListing(market)

        stocks = []
        for _, row in df_stocks.iterrows():
            stock = StockItem(
                symbol=row.get('Code', ''),
                name=row.get('Name', ''),
                market=market
            )
            stocks.append(stock)

        return stocks
