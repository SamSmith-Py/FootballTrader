import betfairlightweight
from betfairlightweight import filters
from datetime import datetime, timedelta, timezone

api = betfairlightweight.APIClient('smudge2049', 'Nocommsnobombs17!', '4oAYsDJiYA7P5Wej')
api.login_interactive()

# market_books = api.betting.list_market_book(market_ids=['1.251909779'], price_projection={'priceData': ['EX_ALL_OFFERS', 'SP_AVAILABLE']})

# print(market_books[0].runners[2].ex.available_to_lay[0].price)

market_filter = filters.market_filter(
            event_type_ids=[1],  # 1 = Soccer
            market_type_codes=['MATCH_ODDS'],
            in_play_only=False,
            market_start_time={
                "from": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": (datetime.now(timezone.utc) + timedelta(hours=12)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
cats = api.betting.list_market_catalogue(
            filter=market_filter,
            market_projection=["COMPETITION", "EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION"],
            max_results=500,
            lightweight=False,
        )
for x in cats:
    print(x.competition.id, x.competition.name, x.event.country_code)