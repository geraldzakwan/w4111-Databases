import HW_Assignments.Final.transactions as transactions


def t1():

    new_id = transactions.create_account(111.0, cursor=None)
    print("ID for new account = ", new_id)


def t2():

    balance = transactions.get_balance(5, cursor=None)
    print("Balance for account ", 5, " is ", balance)


def t3():

    acct = transactions.get_account(5, cursor=None)
    print("Account information for account ", 5, " is ", acct)


t1()
#t2()
#t3()