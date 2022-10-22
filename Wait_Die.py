
import pandas as pd

global TS
TS = 0


def checkTransExists(TransId):
    global TransTable
    global LockTable
    global BlockTable

    transList = TransTable.loc[TransTable['TransactionID'] == TransId, ['TransactionID', 'TransactionTimeStamp', 'TransactionStatus']]

    if transList.size == 0:
        return False
    return True


def getTransStatus(TransId):
    global TransTable
    global LockTable
    global BlockTable

    transList = TransTable.loc[TransTable['TransactionID'] == TransId, ['TransactionID', 'TransactionTimeStamp', 'TransactionStatus']]
    return transList['TransactionStatus'].to_list()[0]



def StartTrans(r, TransId):
    global TransTable
    global LockTable
    global BlockTable
    global TS

    TS = TS + 1
    print("Begin Transaction " + TransId + " :Record is added to Transaction Table with TransId= " + TransId + " ,Timestamp= " + str(
            TS) + " ,State = Active")
    r = r.replace('\n', '')
    TransTable = TransTable.append(pd.Series([TransId, TS, 'Active'], index=TransTable.columns),
                                   ignore_index=True)


def waitDieTrans(TransId, lockList, dtItem, r):
    global TransTable
    global LockTable
    global BlockTable

    currTS = getTS(TransId)
    youngerTransId = []
    olderTransId = []
    for e in lockList['TransactionIDHold'].to_list():
        if (e != TransId):

            if currTS > getTS(e):
                olderTransId.append(e)
            else:
                youngerTransId.append(e)

    if olderTransId and not youngerTransId:
        print("Transaction " + TransId + " is aborted (state = Aborted)")
        abortTrans(TransId)

    if youngerTransId and not olderTransId:
        print("Transaction " + TransId + " goes into blocked state (state = Blocked)")
        TransTable.loc[TransTable['TransactionID'] == TransId, ['TransactionStatus']] = 'Blocked'

    for T in youngerTransId:
        BlockTable = BlockTable.append(pd.Series([TransId, dtItem, r, T], index=BlockTable.columns),
                                       ignore_index=True)

def readLockTrans(r, TransId):
    global TransTable
    global LockTable
    global BlockTable
    r = r.replace('\n', '')
    dtItem = r[3]

    lockL = LockTable.loc[LockTable['ItemName'] == dtItem, ['ItemName', 'Lockstate', 'TransactionIDHold']]


    if lockL.size == 0:
        print(dtItem+" is read locked by "+TransId)
        LockTable = LockTable.append(pd.Series([dtItem, 'R', TransId], index=LockTable.columns),
                                     ignore_index=True)
    else:
        Write = False
        for rows in lockL.iterrows():
            if rows[1][0] == dtItem and rows[1][1] == 'W':
                Write = True
                break


        if Write:
            waitDieTrans(TransId, lockL, dtItem, r)
        else:
            print(dtItem+" is read locked by"+TransId)
            LockTable = LockTable.append(pd.Series([dtItem, 'R', TransId], index=LockTable.columns),
                                         ignore_index=True)



def getTS(TransId):
    global TransTable
    global LockTable
    global BlockTable

    transList = TransTable.loc[
        TransTable['TransactionID'] == TransId, ['TransactionID', 'TransactionTimeStamp', 'TransactionStatus']]

    return transList['TransactionTimeStamp'].to_list()[0]


def abortTrans(TransId):
    global TransTable
    global LockTable
    global BlockTable


    TransTable.loc[TransTable['TransactionID'] == TransId, ['TransactionStatus']] = "Aborted"

    lockL = LockTable.loc[LockTable['TransactionIDHold'] == TransId, ['ItemName', 'Lockstate', 'TransactionIDHold']]

    LockTable = LockTable.loc[LockTable['TransactionIDHold'] != TransId]

    BlockTable = BlockTable.loc[BlockTable['TransactionID'] != TransId]

    opList = BlockTable.loc[BlockTable['BlockedBy'] == TransId, ['TransactionID', 'ItemName', 'Operation', 'BlockedBy']]

    blockedTID = ''
    newBlockedBy = ''
    for r in opList.iterrows():
        id = r[1][0]

        dtItem = r[1][1]
        count = LockTable.loc[(LockTable['ItemName'] == dtItem) & (LockTable['TransactionIDHold'] != id)]

        if len(count) > 0:
            blockedTID = id
            newBlockedBy = count['TransactionIDHold'].to_list()[0]

            opList = BlockTable.loc[
                (BlockTable['BlockedBy'] == TransId) & (BlockTable['TransactionID'] != id), ['TransactionID', 'ItemName',
                                                                                         'Operation', 'BlockedBy']]

    cList = BlockTable.loc[
        (BlockTable['BlockedBy'] == TransId) & (BlockTable['TransactionID'] == blockedTID), ['TransactionID', 'ItemName',
                                                                                         'Operation', 'BlockedBy']]
    for row1 in cList.iterrows():
        id = row1[1][0]

        dtItem = row1[1][1]
        BlockTable.loc[BlockTable['TransactionID'] == blockedTID, ['BlockedBy']] = newBlockedBy

    for r in opList.iterrows():

        id = r[1][0]

        dtItem = r[1][1]

        count = BlockTable.loc[(BlockTable['TransactionID'] == id) & (BlockTable['ItemName'] == dtItem)]

        if (len(count) > 1):
            BlockTable = BlockTable.loc[(BlockTable['ItemName'] != dtItem) | (BlockTable['BlockedBy'] != TransId)]
        else:
            BlockTable = BlockTable.loc[(BlockTable['ItemName'] != dtItem) | (BlockTable['BlockedBy'] != TransId

                                                                          )]
            print(r[1][2] + " in queue are implemented")
            TransTable.loc[TransTable['TransactionID'] == id, ['TransactionStatus']] = 'Active'
            executeFunc(r[1][2], r[1][0])



def commitTrans(TransId):
    global TransTable
    global LockTable
    global BlockTable


    TransTable.loc[TransTable['TransactionID'] == TransId, ['TransactionStatus']] = "Committed"

    lockL = LockTable.loc[LockTable['TransactionIDHold'] == TransId, ['ItemName', 'Lockstate', 'TransactionIDHold']]

    LockTable = LockTable.loc[LockTable['TransactionIDHold'] != TransId]

    BlockTable = BlockTable.loc[BlockTable['TransactionID'] != TransId]

    opList = BlockTable.loc[BlockTable['BlockedBy'] == TransId, ['TransactionID', 'ItemName', 'Operation', 'BlockedBy']]

    blockedTID = ''
    newBlockedBy = ''
    for r in opList.iterrows():
        id = r[1][0]

        dtItem = r[1][1]
        count = LockTable.loc[(LockTable['ItemName'] == dtItem) & (LockTable['TransactionIDHold'] != id)]

        if len(count) > 0:
            blockedTID = id
            newBlockedBy = count['TransactionIDHold'].to_list()[0]

            opList = BlockTable.loc[
                (BlockTable['BlockedBy'] == TransId) & (BlockTable['TransactionID'] != id), ['TransactionID', 'ItemName',
                                                                                         'Operation', 'BlockedBy']]

    cList = BlockTable.loc[
        (BlockTable['BlockedBy'] == TransId) & (BlockTable['TransactionID'] == blockedTID), ['TransactionID', 'ItemName',
                                                                                         'Operation', 'BlockedBy']]
    for r1 in cList.iterrows():
        id = r1[1][0]

        dtItem = r1[1][1]
        BlockTable.loc[BlockTable['TransactionID'] == blockedTID, ['BlockedBy']] = newBlockedBy

    for r in opList.iterrows():

        id = r[1][0]

        dtItem = r[1][1]

        count = BlockTable.loc[(BlockTable['TransactionID'] == id) & (BlockTable['ItemName'] == dtItem)]

        if (len(count) > 1):
            BlockTable = BlockTable.loc[(BlockTable['ItemName'] != dtItem) | (BlockTable['BlockedBy'] != TransId)]
        else:


            BlockTable = BlockTable.loc[(BlockTable['ItemName'] != dtItem) | (BlockTable['BlockedBy'] != TransId)]
            print(r[1][2] + " in queue are implemented")
            TransTable.loc[TransTable['TransactionID'] == id, ['TransactionStatus']] = 'Active'
            executeFunc(r[1][2], r[1][0])



def writeLockTrans(r, TransId):
    global TransTable
    global LockTable
    global BlockTable
    r = r.replace('\n', '')
    dtItem = r[3]

    lockL = LockTable.loc[LockTable['ItemName'] == dtItem, ['ItemName', 'Lockstate', 'TransactionIDHold']]

    if len(lockL) == 0:
        print(dtItem+" is write locked by "+TransId)
        LockTable = LockTable.append(pd.Series([dtItem, 'W', TransId], index=LockTable.columns),
                                     ignore_index=True)

    elif len(lockL) == 1 and lockL['TransactionIDHold'].to_list()[0] == TransId and lockL['Lockstate'].to_list()[
        0] == 'R':
        print("Transaction " + TransId + " updated from read lock to write lock on data item " + dtItem)
        LockTable.loc[LockTable['ItemName'] == dtItem, ['Lockstate']] = 'W'
    else:
        currTS = getTS(TransId)

        youngerTransId = []
        olderTransId = []
        for e in lockL['TransactionIDHold'].to_list():
            if (e != TransId):

                if currTS > getTS(e):
                    olderTransId.append(e)
                else:
                    youngerTransId.append(e)

        waitDieTrans(TransId, lockL, dtItem, r)

def addToQueue(TransId, r):
    global TransTable
    global LockTable
    global BlockTable

    opList = BlockTable.loc[BlockTable['TransactionID'] == TransId,['TransactionID','ItemName','Operation','BlockedBy']]
    for operation in opList['BlockedBy'].to_list():
        BlockTable = BlockTable.append(pd.Series([TransId, r[3], r, operation], index=BlockTable.columns ), ignore_index=True)

def addToQueue1(TransId, r):
    global TransTable
    global LockTable
    global BlockTable

    opList = BlockTable.loc[
        BlockTable['TransactionID'] == TransId, ['TransactionID', 'ItemName', 'Operation', 'BlockedBy']]
    for operation in opList['BlockedBy'].to_list():
        BlockTable = BlockTable.append(pd.Series([TransId, '', r, operation], index=BlockTable.columns),
                                       ignore_index=True)



def executeFunc(r, TransId):
    if r[0] == 'b':
        if not checkTransExists(TransId):
            StartTrans(r, TransId)

    if r[0] == 'r':
        if checkTransExists(TransId):
            s = getTransStatus(TransId)

            if (s == 'Active'):
                readLockTrans(r, TransId)
            elif (s == 'Aborted'):
                print('Transaction ' + TransId + ' is Aborted already. No modifications in tables')
            elif (s == 'Blocked'):
                print(r + " added to queue as transaction " + TransId + " is in " + s + " state")
                addToQueue(TransId, r)

    if r[0] == 'w':
        if checkTransExists(TransId):
            s = getTransStatus(TransId)
            if (s == 'Active'):
                writeLockTrans(r, TransId)
            elif (s == 'Aborted'):
                print('Transaction ' + TransId + ' is Aborted already. No modifications in tables')
            elif (s == 'Blocked'):
                print(r+ " added to queue as transaction " + TransId + " is in " + s + " state")
                addToQueue(TransId, r)

    if r[0] == 'e':
        if checkTransExists(TransId):
            s = getTransStatus(TransId)
            if (s == 'Aborted'):
                print('Transaction ' + TransId + ' is Aborted already. No modifications in tables')
            elif (s == 'Blocked'):
                print(r + " added to queue as transaction " + TransId + " is in " + s + " state")
                addToQueue1(TransId, r)
            elif (s == "Committed"):
                print('Transaction: ' + TransId + ' is committed already')
            else:
                print("Transaction " + TransId + " committed")
                commitTrans(TransId)


def main():
    global TransTable
    global LockTable
    global BlockTable
    LockTable = pd.DataFrame(columns=['ItemName', 'Lockstate', 'TransactionIDHold'])
    TransTable = pd.DataFrame(columns=['TransactionID', 'TransactionTimeStamp', 'TransactionStatus'])
    BlockTable = pd.DataFrame(columns=['TransactionID', 'ItemName', 'Operation', 'BlockedBy'])
    InputFName = input(
        "\nEnter the input file Name, please add '.txt' to end of the file name: \n\n")


    try:
        inputF = open(InputFName, 'r')
        for r in inputF.readlines():
            if r:
                r = r.replace(' ', '')
                if r != '\n':
                    currTrans = 'T' + r[1]
                    r = r.replace('\n', '')
                    print(r)
                    executeFunc(r, currTrans)

    except:
        print("Please enter the correct file name")
        main()




if __name__ == '__main__':
    main() 