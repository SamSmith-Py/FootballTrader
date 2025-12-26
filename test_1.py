import betfairlightweight

api = betfairlightweight.APIClient('smudge2049', 'Nocommsnobombs17!', '4oAYsDJiYA7P5Wej')
api.login_interactive()

market_books = api.betting.list_market_book(market_ids=['1.251819993'], price_projection={'priceData': ['EX_ALL_OFFERS', 'SP_AVAILABLE']})

print(market_books[0].runners[0].ex.available_to_back[0].price)
a = market_books[0].runners[1].sp.far_price
print(a)
