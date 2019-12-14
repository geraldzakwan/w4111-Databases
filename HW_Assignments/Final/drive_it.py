import sys

sys.path.append("/Users/donaldferguson/OneDrive - ANSYS, Inc/Columbia/W4111f19/w4111-Databases")

import HW_Assignments.Final.transactions as trans


def drive_it():

    tran_approach = int(input("Optimistic = 1, Pessimistic = 2: "))
    if tran_approach == 1:
        tran_approach = "OPTIMISTIC"
    else:
        tran_approach = "PESSIMISTIC"

    print("Transaction approach = ", tran_approach)

    if tran_approach == "PESSIMISTIC":
        trans.transfer_pessimistic()
    else:
        trans.transfer_optimistic()


drive_it()