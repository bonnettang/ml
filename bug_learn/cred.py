import pandas as pd
from datetime import datetime
class CreditCard(object):
    def __init__(self,limit=0,temp_limit=0,grace_period=0,statement_date=None):
        self.limit=limit
        self.temp_limit=temp_limit
        self.current_usage=0
        self.grace_period=grace_period
        self.statement_date=statement_date
        self.repayment_date=None
        self.account_money=0

    def pay(self,money):
        if self.limit+self.temp_limit-self.current_usage-money<0:
            print('chao biao le')
            return False
        self.current_usage+=money
        print('success')
        print('sddessfffdddeeeww')
        return True
    def pay_back(self,money):
        self.current_usage=self.current_usage-money
        self.account_money=self.account_money-money
    def generate_account_money(self):
        self.account_money=self.current_usage

class Wealth(object):
    def __init__(self,years_rate=60,period=0,begin_date=None):
        self.rate=years_rate
        self.period=period
        self.begin_date=begin_date

class Pos(object):
    def __init__(self,rate=55):
        self.rate=float(rate)/10000
    def pay(self,money):
        return (money-money*self.rate,money*self.rate)

class Strategy(object):
    def pay_with_card(self,card, pos, money):
        card = CreditCard()
        pos = Pos()
        if card.pay(money):
            result = pos.pay(money)
            return result
        else:
            return (0, 0)
    #meigeyue 10 times, 2 big, 2 mide 6 small,  big zhangdanri 5 tian nei, middle zhangdan 20 tian nei, small zhangdan 30 tian nei
    #meigeka  dou you shua, bu chao guo e du 30%

if __name__ == '__main__':
    #generate card info
    icbc=CreditCard(limit=67000,grace_period=56,statement_date=12)
    ky=Pos(55)
