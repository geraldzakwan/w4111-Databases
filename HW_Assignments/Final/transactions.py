import uuid

import HW_Assignments.Final.dbutils as dbutils


def get_cursor(cursor, isolation_level="SERIALIZABLE"):
    """

    :param cursor: A cursor, which may be None.
    :param isolation_level: A valid SQL isolation level as a string
    :return: (True/False, cursor, connection)

    This function simply returns (False, the input cursor) if passed a cursor that is not None. Otherwise,
    it creates a cursor, sets the isolation level and returns (True, created cursor, created_connection).
    """

    # I repeat this segment of code over and over again in each function just for clarity.
    # Normally, I would put in a function shared by all transactional functions. If I did this,
    # you would have to look through two functions to understand the logic.
    #
    # Is there already a cursor?
    if cursor is None:
        # Not part of a bigger transaction. Create connection and cursor.
        cnx = dbutils.get_new_connection()
        cursor = cnx.cursor()
        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL " +  isolation_level)
        cursor_created = True
    else:
        cursor_created = False
        cnx = None

    return cursor_created, cursor, cnx


def process_transaction(cursor_created, cursor, conn, success=True):
    """

    :param cursor_created: True if the function calling created a cursor.
    :param cursor: Cursor for the calling functions transactions
    :param conn: Connection for the calling function, may be None.
    :param success: Did the function execute successfully?
    :return: None
    """

    # The calling function created a cursor and connection, and needs to handle final processing.
    if cursor_created == True:

        # If the outcome was successful.
        if success == True:
            conn.commit()
            conn.close()
        else:
            conn.rollback()
            conn.close()
    else:
        # The calling function did not open the cursor/start a transaction.
        # It is part of a larger application that will manage transacton outcome.
        pass


def create_account(balance, cursor):
    """

    :param balance: The initial balance of the account.
    :param cursor: If not None, this call is part of a larger application that will manage transactions
                    and isolation levels.
    :return: ID of the created count.
    """

    cursor_created, cursor, conn = get_cursor(cursor)

    try:
        # Generate a UUID for this version of the account state.
        version_id = str(uuid.uuid4())

        # Insert/create the new account record.
        q = "insert into w4111_f19_final.banking_account (balance, version_id) values(%s, %s)"

        #sql, args = None, fetch = True, cur = None, conn = final_conn, commit = True, debug = False)
        result = dbutils.run_q(q, args=(balance, version_id), fetch=True, commit=False, cur=cursor)

        # By definition, we are in a transaction. So, auto-increment must be the largest value for
        # the auto-increment field.
        q = "select max(id) as new_id from w4111_f19_final.banking_account;"
        result = dbutils.run_q(q, args=None, fetch=True, commit=False, cur=cursor)

        result = result[1][0]['new_id']

        # Handle transaction behavior on success.
        process_transaction(cursor_created, cursor, conn, True)
    except Exception as e:
        print("Exception = ", e)
        result = None
        process_transaction(cursor_created, cursor_created, conn, False)
        raise e

    return result


def get_balance(id, cursor=None):
    """

    Gets the balance for an account given an id. This call may be part of a larger transaction,
    and will receive a cursor if it is. Otherwise, cursor is None.
    :param id: The account number.
    :param cursor: Cursor for larger transaction, if any.
    :return:
    """

    cursor_created, cursor, conn = get_cursor(cursor)

    try:
        # Get the account balance.
        q = "select * from w4111_f19_final.banking_account where id=%s"
        result = dbutils.run_q(q, args=id, fetch=True, commit=False, cur=cursor)

        process_transaction(cursor_created, cursor, conn, True)

    except Exception as e:
        print("Exception = ", e)
        result = None
        process_transaction(cursor_created, cursor_created, conn, False)
        raise e

    return result[1][0]['balance']


def get_account(id, cursor=None):
    """

    Gets the balance for an account given an id. This call may be part of a larger transaction,
    and will receive a cursor if it is. Otherwise, cursor is None.
    :param id: The account number.
    :param cursor: Cursor for larger transaction, if any.
    :return: The account information
    """

    cursor_created, cursor, conn = get_cursor(cursor)

    try:
        # Get the account balance.
        q = "SELECT * FROM w4111_f19_final.banking_account WHERE id=%s"
        result = dbutils.run_q(q, args=id, fetch=True, commit=False, cur=cursor)

        process_transaction(cursor_created, cursor, conn, True)

    except Exception as e:
        print("Exception = ", e)
        result = None
        process_transaction(cursor_created, cursor_created, conn, False)
        raise e

    return result[1][0]


def update_balance(id, amount, cursor=None):
    """

    :param id: Account number.
    :param amount: New balance to set.
    :param cursor: Cursor if part of a larger transaction. None otherwise.
    :return:
    """

    cursor_created, cursor, conn = get_cursor(cursor)

    try:
        # This function is going to change the data, and needs to modify version information.
        new_version = str(uuid.uuid4())

        # Update balance and version number.
        q = "update w4111_f19_final.banking_account set balance=%s, version_id=%s where id=%s"
        result = dbutils.run_q(q, args=(amount, new_version, id), fetch=True, commit=False, cur=cursor)

        process_transaction(cursor_created, cursor, conn, True)

    except Exception as e:
        print("Exception = ", e)
        result = None
        process_transaction(cursor_created, cursor_created, conn, False)
        raise e

    return result


def update_balance_optimistic(acct, amount, cursor=None):

    cursor_created, cursor, conn = get_cursor(cursor)

    try:
        current_acct = get_account(acct['id'], cursor=cursor)
        if current_acct['version_id'] != acct['version_id']:
            raise ValueError("Optimistic transaction failed.")

        new_version = str(uuid.uuid4())

        q = "update w4111_f19_final.banking_account set balance=%s, version_id=%s where id=%s"

        result = dbutils.run_q(q, args=(amount, new_version, acct['id']), fetch=True, commit=False, cur=cursor)

        process_transaction(cursor_created, cursor, conn, True)

    except Exception as e:
        print("Exception = ", e)
        result = None
        process_transaction(cursor_created, cursor_created, conn, False)
        raise e

    return result


def transfer_pessimistic():
    """
    Prompts for source and target accounts and amount to transfer.
    Locks accounts to prevent another update from interfering during the transfer.
    :return:
    """

    print(" \n*** Transfering Pessimistically ***\n")

    cursor_created, cursor, conn = get_cursor(None)

    try:

        # Get the source account information.
        source_id = input("Source account ID: ")

        # Get balance. Since we are passing a cursor, there will be a read lock on the account tuples.
        source_b = get_balance(source_id, cursor=cursor)

        # I do these prompts this way to slow down the transaction so that we can play with various conflicts.
        cont = input("Source balance = " + str(source_b) + ". Continue (y/n)")

        if cont == 'y':

            # Same logic but for target.
            target_id = input("Target account ID: ")
            target_b = get_balance(target_id, cursor=cursor)
            input("Target balance = " + str(target_b) + ". Continue (y/n)")

            if cont == 'y':
                amount = input("Amount: ")
                amount = float(amount)

                # Compute new balances.
                new_source = source_b - amount
                new_target = target_b + amount

                # Perform updates.
                update_balance(source_id, new_source, cursor=cursor)
                update_balance(target_id, new_target, cursor=cursor)

            process_transaction(cursor_created, cursor, conn, True)
        else:
            process_transaction(cursor_created, cursor, conn, False)

    except Exception as e:
        print("Exception = ", e)
        result = None
        process_transaction(cursor_created, cursor_created, conn, False)
        raise e

    return


def transfer_optimistic():
    """
    Same as above, but optimistic.
    :return:
    """

    print(" \n*** Transfering Optimistically *** \n")

    source_id = input("Source account ID: ")

    # Do not pass a cursor. Read should read, commit and release locks.
    source_acct = get_account(source_id, cursor=None)
    cont = input("Source balance = " + str(source_acct['balance']) + ". Continue (y/n)")

    result = None

    if cont == 'y':

        # Same basic logic.
        target_id = input("Target account ID: ")
        target_acct = get_account(target_id, cursor=None)
        input("Target balance = " + str(target_acct['balance']) + ". Continue (y/n)")

        if cont == 'y':

            amount = input("Amount: ")
            amount = float(amount)


            cursor_created, cursor, conn = get_cursor(None)# Compute new balances.
            new_source = source_acct['balance'] - amount
            new_target = target_acct['balance'] + amount

            try:
                # Begin a transaction to perform transfer.

                # Update the balances. This will fail if underlying balance has changed.
                update_balance_optimistic(source_acct, new_source, cursor=cursor)
                update_balance_optimistic(target_acct, new_target, cursor=cursor)

                process_transaction(cursor_created, cursor, conn, True)

                result = amount

            except Exception as e:
                print("Exception = ", e)
                result = None
                process_transaction(cursor_created, cursor_created, conn, False)
                raise e


    return result



