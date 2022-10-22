import pandas as pd

global timeStamp
timeStamp = 0


def To_Check_Existing_Transactions(TransId):
    global Transactions_DF
    transactionList = Transactions_DF.loc[
        Transactions_DF['TransactionIDNumber'] == TransId, ['TransactionIDNumber', 'TimeStamp_of_Transaction',
                                                        'Current_Transaction_Status']]

    if transactionList.size == 0:
        return False
    return True


def Transaction_Assignment(r, TransId):
    global Transactions_DF
    global timeStamp

    timeStamp = timeStamp + 1

    print(
        "Begin Transaction " + TransId + " :Record is added to Transaction Table with TID= " + TransId + " and timestamp= " + str(
            timeStamp) + " and State_of_Transaction = active")
    r = r.replace('\n', '')
    Transactions_DF = Transactions_DF.append(pd.Series([TransId, timeStamp, 'Active'], index=Transactions_DF.columns),
                                             ignore_index=True)


def Apply_CW_Locking(TransId, lockList, dataDataItems, r):
    global Transactions_DF
    global Wait_DF

    tidList = []
    Block_Exist = False

    for idnumber in lockList['Holding_TransactionIDNumber'].to_list():
        if (idnumber != TransId):
            status = Transactions_DF.loc[
                Transactions_DF['TransactionIDNumber'] == idnumber, ['TransactionIDNumber', 'TimeStamp_of_Transaction',
                                                                     'Current_Transaction_Status']]
            status = status['Current_Transaction_Status'].to_list()[0]
            tidList.append(idnumber)
            if status == 'Blocked':
                Block_Exist = True

    if Block_Exist:
        print("Transaction " + TransId + " is aborted (State_of_Transaction = Aborted)\n")
        abort(TransId)

    else:
        print("Transaction " + TransId + "goes into blocked State_of_Transaction (State_of_Transaction = Blocked)")
        Transactions_DF.loc[Transactions_DF['TransactionIDNumber'] == TransId, ['Current_Transaction_Status']] = 'Blocked'
        for tid in tidList:
            Wait_DF = Wait_DF.append(pd.Series([TransId, dataDataItems, r, tid], index=Wait_DF.columns), ignore_index=True)


def apply_read_lock(r, TransId):
    global Locks_DF

    r = r.replace('\n', '')
    dataDataItems = r[3]

    lockList = Locks_DF.loc[
        Locks_DF['DataItems'] == dataDataItems, ['DataItems', 'State_of_Transaction', 'Holding_TransactionIDNumber']]

    if lockList.size == 0:
        print(dataDataItems+" is read locked by"+TransId)
        Locks_DF = Locks_DF.append(pd.Series([dataDataItems, 'R', TransId], index=Locks_DF.columns), ignore_index=True)
    else:
        atleastOneWrite = False
        for row1 in lockList.iterrows():
            if row1[1][0] == dataDataItems and row1[1][1] == 'W':
                atleastOneWrite = True
                break

        if atleastOneWrite:
            Apply_CW_Locking(TransId, lockList, dataDataItems, r)
        else:
            print(dataDataItems+" is read locked by"+TransId)
            Locks_DF = Locks_DF.append(pd.Series([dataDataItems, 'R', TransId], index=Locks_DF.columns), ignore_index=True)


def abort(TransId):
    global Transactions_DF
    global Locks_DF
    global Wait_DF

    Transactions_DF.loc[Transactions_DF['TransactionIDNumber'] == TransId, ['Current_Transaction_Status']] = "Aborted"

    lockList = Locks_DF.loc[Locks_DF['Holding_TransactionIDNumber'] == TransId, ['DataItems', 'State_of_Transaction',
                                                                             'Holding_TransactionIDNumber']]

    Locks_DF = Locks_DF.loc[Locks_DF['Holding_TransactionIDNumber'] != TransId]

    Wait_DF = Wait_DF.loc[Wait_DF['TransactionIDNumber'] != TransId]

    operationList = Wait_DF.loc[
        Wait_DF['Blocked_by_Transaction'] == TransId, ['TransactionIDNumber', 'DataItems', 'operation',
                                                   'Blocked_by_Transaction']]

    blockedTID = ''
    newBlocked_by_Transaction = ''
    for r in operationList.iterrows():
        id = r[1][0]

        dataDataItems = r[1][1]
        count = Locks_DF.loc[(Locks_DF['DataItems'] == dataDataItems) & (Locks_DF['Holding_TransactionIDNumber'] != id)]

        if len(count) > 0:
            blockedTID = id
            newBlocked_by_Transaction = count['Holding_TransactionIDNumber'].to_list()[0]

            operationList = Wait_DF.loc[
                (Wait_DF['Blocked_by_Transaction'] == TransId) & (Wait_DF['TransactionIDNumber'] != id), [
                    'TransactionIDNumber', 'DataItems', 'operation', 'Blocked_by_Transaction']]

    changedList = Wait_DF.loc[
        (Wait_DF['Blocked_by_Transaction'] == TransId) & (Wait_DF['TransactionIDNumber'] == blockedTID), [
            'TransactionIDNumber', 'DataItems', 'operation', 'Blocked_by_Transaction']]
    for row1 in changedList.iterrows():
        id = row1[1][0]

        dataDataItems = row1[1][1]
        Wait_DF.loc[
            Wait_DF['TransactionIDNumber'] == blockedTID, ['Blocked_by_Transaction']] = newBlocked_by_Transaction

    for r in operationList.iterrows():

        id = r[1][0]

        dataDataItems = r[1][1]

        count = Wait_DF.loc[(Wait_DF['TransactionIDNumber'] == id) & (Wait_DF['DataItems'] == dataDataItems)]

        if (len(count) > 1):
            Wait_DF = Wait_DF.loc[(Wait_DF['DataItems'] != dataDataItems) | (Wait_DF['Blocked_by_Transaction'] != TransId)]
        else:
            Wait_DF = Wait_DF.loc[(Wait_DF['DataItems'] != dataDataItems) | (Wait_DF['Blocked_by_Transaction'] != TransId)]
            print("Operations " + r[1][2] + " in queue are implemented\n")
            Transactions_DF.loc[Transactions_DF['TransactionIDNumber'] == id, ['Current_Transaction_Status']] = 'Active'
            Check_type_of_Transaction(r[1][2], r[1][0])


def commit(TransId):
    global Transactions_DF
    global Locks_DF
    global Wait_DF

    Transactions_DF.loc[Transactions_DF['TransactionIDNumber'] == TransId, ['Current_Transaction_Status']] = "Committed"

    lockList = Locks_DF.loc[Locks_DF['Holding_TransactionIDNumber'] == TransId, ['DataItems', 'State_of_Transaction',
                                                                             'Holding_TransactionIDNumber']]

    Locks_DF = Locks_DF.loc[Locks_DF['Holding_TransactionIDNumber'] != TransId]

    Wait_DF = Wait_DF.loc[Wait_DF['TransactionIDNumber'] != TransId]

    operationList = Wait_DF.loc[
        Wait_DF['Blocked_by_Transaction'] == TransId, ['TransactionIDNumber', 'DataItems', 'operation',
                                                   'Blocked_by_Transaction']]

    blockedTID = ''
    newBlocked_by_Transaction = ''
    for r in operationList.iterrows():
        id = r[1][0]

        dataDataItems = r[1][1]
        count = Locks_DF.loc[(Locks_DF['DataItems'] == dataDataItems) & (Locks_DF['Holding_TransactionIDNumber'] != id)]

        if len(count) > 0:
            blockedTID = id
            newBlocked_by_Transaction = count['Holding_TransactionIDNumber'].to_list()[0]

            operationList = Wait_DF.loc[
                (Wait_DF['Blocked_by_Transaction'] == TransId) & (Wait_DF['TransactionIDNumber'] != id), [
                    'TransactionIDNumber', 'DataItems', 'operation', 'Blocked_by_Transaction']]

    changedList = Wait_DF.loc[
        (Wait_DF['Blocked_by_Transaction'] == TransId) & (Wait_DF['TransactionIDNumber'] == blockedTID), [
            'TransactionIDNumber', 'DataItems', 'operation', 'Blocked_by_Transaction']]
    for row1 in changedList.iterrows():
        id = row1[1][0]

        dataDataItems = row1[1][1]
        Wait_DF.loc[
            Wait_DF['TransactionIDNumber'] == blockedTID, ['Blocked_by_Transaction']] = newBlocked_by_Transaction

    for r in operationList.iterrows():

        id = r[1][0]

        dataDataItems = r[1][1]

        count = Wait_DF.loc[(Wait_DF['TransactionIDNumber'] == id) & (Wait_DF['DataItems'] == dataDataItems)]

        if (len(count) > 1):
            Wait_DF = Wait_DF.loc[(Wait_DF['DataItems'] != dataDataItems) | (Wait_DF['Blocked_by_Transaction'] != TransId)]
        else:
            # execute the operations which are blocked by current transaction and change the transaction status to "Active"
            Wait_DF = Wait_DF.loc[(Wait_DF['DataItems'] != dataDataItems) | (Wait_DF['Blocked_by_Transaction'] != TransId)]
            print("Operations " + r[1][2] + " in queue are implemented\n")
            Transactions_DF.loc[Transactions_DF['TransactionIDNumber'] == id, ['Current_Transaction_Status']] = 'Active'
            Check_type_of_Transaction(r[1][2], r[1][0])


def apply_write_lock(r, TransId):
    global Locks_DF

    r = r.replace('\n', '')
    dataDataItems = r[3]

    lockList = Locks_DF.loc[
        Locks_DF['DataItems'] == dataDataItems, ['DataItems', 'State_of_Transaction', 'Holding_TransactionIDNumber']]

    if len(lockList) == 0:
        print(dataDataItems + " is write locked by" + TransId)
        Locks_DF = Locks_DF.append(pd.Series([dataDataItems, 'W', TransId], index=Locks_DF.columns), ignore_index=True)
    elif len(lockList) == 1 and lockList['Holding_TransactionIDNumber'].to_list()[0] == TransId and \
            lockList['State_of_Transaction'].to_list()[0] == 'R':
        print("Transaction " + TransId + " updated from read lock to write lock on DataItems " + dataDataItems+"\n")
        Locks_DF.loc[Locks_DF['DataItems'] == dataDataItems, ['State_of_Transaction']] = 'W'
    else:

        Apply_CW_Locking(TransId, lockList, dataDataItems, r)


def read_write_operations_ToQueue(TransId, r):
    global Wait_DF

    operationList = Wait_DF.loc[Wait_DF['TransactionIDNumber'] == TransId, ['TransactionIDNumber', 'DataItems', 'operation',
                                                                        'Blocked_by_Transaction']]
    for operation in operationList['Blocked_by_Transaction'].to_list():
        Wait_DF = Wait_DF.append(pd.Series([TransId, r[3], r, operation], index=Wait_DF.columns), ignore_index=True)


def end_operation_ToQueue(TransId, r):
    global Wait_DF

    operationList = Wait_DF.loc[Wait_DF['TransactionIDNumber'] == TransId, ['TransactionIDNumber', 'DataItems', 'operation',
                                                                        'Blocked_by_Transaction']]
    for operation in operationList['Blocked_by_Transaction'].to_list():
        Wait_DF = Wait_DF.append(pd.Series([TransId, '', r, operation], index=Wait_DF.columns), ignore_index=True)


def check_b_operation(r, TransId):
    if not To_Check_Existing_Transactions(TransId):
        Transaction_Assignment(r, TransId)


def check_r_operation(r, TransId):
    if To_Check_Existing_Transactions(TransId):
        status = Transactions_DF.loc[
            Transactions_DF['TransactionIDNumber'] == TransId, ['TransactionIDNumber', 'TimeStamp_of_Transaction',
                                                            'Current_Transaction_Status']]
        status = status['Current_Transaction_Status'].to_list()[0]
        if (status == 'Active'):
            apply_read_lock(r, TransId)
        elif (status == 'Aborted'):
            print('Transaction ' + TransId + ' is Aborted already. No modifications in tables\n')
        elif (status == 'Blocked'):
            print(
                "Operation " + r + " added to queue as transaction " + TransId + " is in " + status + " State_of_Transaction\n")
            read_write_operations_ToQueue(TransId, r)


def check_w_operation(r, TransId):
    if To_Check_Existing_Transactions(TransId):
        status = Transactions_DF.loc[
            Transactions_DF['TransactionIDNumber'] == TransId, ['TransactionIDNumber', 'TimeStamp_of_Transaction',
                                                            'Current_Transaction_Status']]
        status = status['Current_Transaction_Status'].to_list()[0]
        if (status == 'Active'):
            apply_write_lock(r, TransId)
        elif (status == 'Aborted'):
            print('Transaction ' + TransId + ' is Aborted already.No changes in tables\n')
        elif (status == 'Blocked'):
            print(
                "Operation " + r + " added to queue as transaction " + TransId + " is in " + status + " State_of_Transaction")
            read_write_operations_ToQueue(TransId, r)


def check_e_operation(r, TransId):
    if To_Check_Existing_Transactions(TransId):
        status = Transactions_DF.loc[
            Transactions_DF['TransactionIDNumber'] == TransId, ['TransactionIDNumber', 'TimeStamp_of_Transaction',
                                                            'Current_Transaction_Status']]
        status = status['Current_Transaction_Status'].to_list()[0]
        if (status == 'Aborted'):
            print('Transaction ' + TransId + ' is Aborted already.No modifications in tables\n')
        elif (status == 'Blocked'):
            print(
                "Operation " + r + " added to queue as transaction " + TransId + " is in " + status + " State_of_Transaction\n")
            end_operation_ToQueue(TransId, r)
        elif (status == "Committed"):
            print('Transaction: ' + TransId + ' is committed already')
        else:
            print("Transaction " + TransId + " committed")
            commit(TransId)


def Check_type_of_Transaction(r, TransId):
    if r[0] == 'b':
        check_b_operation(r, TransId)
    if r[0] == 'r':
        check_r_operation(r, TransId)

    if r[0] == 'w':
        check_w_operation(r, TransId)
    if r[0] == 'e':
        check_e_operation(r, TransId)


def main():
    global Transactions_DF
    global Locks_DF
    global Wait_DF
    Locks_DF = pd.DataFrame(columns=['DataItems', 'State_of_Transaction', 'Holding_TransactionIDNumber'])
    Transactions_DF = pd.DataFrame(
        columns=['TransactionIDNumber', 'TimeStamp_of_Transaction', 'Current_Transaction_Status'])
    Wait_DF = pd.DataFrame(columns=['TransactionIDNumber', 'DataItems', 'operation', 'Blocked_by_Transaction'])

    InputFName = input("\nEnter the input file Name, please add '.txt' to end of the file name: \n")
    try:
        inputF = open(InputFName, 'r')
        for r in inputF.readlines():
            if r:
                r = r.replace(' ', '')
                if r != '\n':
                    currentTransaction = 'T' + r[1]
                    print("\n")
                    r = r.replace('\n', '')
                    print("operation: " + r)
                    Check_type_of_Transaction(r, currentTransaction)


    except:
        print("Please enter the correct name, it is a case sensitive. Make sure to add '.txt' to end of the file name")
        main()


if __name__ == '__main__':
    main() 