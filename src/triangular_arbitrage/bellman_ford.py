from typing import Tuple, List
from math import log
import math
import ccxt

CURRENCIES_LETTERS = ('POND', 'BNB', 'BUSD', 'BTC', 'USDT')

#disgusting way I know, but needed to make just for prof of concept
def create_matrix():
    matrix = [[0 for x in range(5)] for y in range(5)]
    for x in range(0,5):
        if x == 1:
            base = "BNB"
        elif x == 2:
            base = "BUSD"
        elif x == 3:
            base = "BTC"
        elif x == 4:
            base = "USDT"
        else:
            base = "POND"
        for y in range(0,5):
            if y == 1:
                intermediate = "BNB"
            elif y == 2:
                intermediate = "BUSD"
            elif y == 3:
                intermediate = "BTC"
            elif y == 4:
                intermediate = "USDT"
            else:
                intermediate = "POND"
            
            if intermediate == base:
                current_ticker_details = 1
            else:
                s1 = f'{intermediate}/{base}'
                try:
                    current_ticker_details = binance.fetch_ticker(s1)
                    current_ticker_details = current_ticker_details['close']
                except:
                    current_ticker_details = math.inf
            matrix[x][y] = current_ticker_details
    return matrix
            
def negate_logarithm_converter(graph: Tuple[Tuple[float]]) -> List[List[float]]:
    result = [[-log(edge) for edge in row] for row in graph]
    return result

def bellman_ford(currency_tuple: tuple, rates_matrix: Tuple[Tuple[float, ...]]):
    trans_graph = negate_logarithm_converter(rates_matrix)

    source = 0
    n = len(trans_graph)
    min_dist = [float('inf')] * n

    pre = [-1] * n
    
    min_dist[source] = source

    for _ in range(n-1):
        for source_curr in range(n):
            for dest_curr in range(n):
                if min_dist[dest_curr] > min_dist[source_curr] + trans_graph[source_curr][dest_curr]:
                    min_dist[dest_curr] = min_dist[source_curr] + trans_graph[source_curr][dest_curr]
                    pre[dest_curr] = source_curr

    # if we can still relax edges, then we have a negative cycle
    for source_curr in range(n):
        for dest_curr in range(n):
            if min_dist[dest_curr] > min_dist[source_curr] + trans_graph[source_curr][dest_curr]:
                # negative cycle exists, and use the predecessor chain to print the cycle
                print_cycle = [dest_curr, source_curr]
                # Start from the source and go backwards until you see the source vertex again or any vertex that already exists in print_cycle array
                while pre[source_curr] not in  print_cycle:
                    print_cycle.append(pre[source_curr])
                    source_curr = pre[source_curr]
                print_cycle.append(pre[source_curr])
                print("Arbitrage Opportunity: \n")
                print(" --> ".join([currencies[p] for p in print_cycle[::-1]]))

if __name__ == "__main__":
    binance = ccxt.binance()
    rates_matrix = create_matrix()
    bellman_ford(CURRENCIES_LETTERS,rates_matrix)
    


        
