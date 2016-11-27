import pandas as pd
from qpython import qconnection
import numpy as np
import os
def int_to_code(i):
    return '0' * (6 - len(str(int(i)))) + str(int(i))

def float_to_100int(f):
    return int(f/100)*100

def update_data_2kdb(data, host, port):
    q = qconnection.QConnection(host=host, port=port, pandas=True)
    q.open()
    q('upsert', np.string_("account"), data)
    q.close()

def caculate_stks(diretory, filename):
    pd.options.mode.chained_assignment = None  # default='warn' 关闭警告log
    rootdir = './'+ diretory +'/'
    summarydir='summary'
    fp=None
    df=pd.read_excel(rootdir+filename,sheetname=None)
    if not os.path.exists(rootdir + summarydir):
        os.makedirs(rootdir + summarydir)
        fp=None
        for key in df:
            print(key)
            pdtemp=df[key]
            pdtemp.columns=pdtemp.ix[0].values
            pdtemp=pdtemp[1:]
            pdtemp=(pdtemp.dropna(how='all')).T.dropna(how='all').T
            if fp is not None:
                fp=fp.append(pdtemp)
            else:
                fp=pdtemp
    #print(fp)
    fp = fp.drop(labels=['stockname','available_num','allocated_num','unalocated_num'],axis=1)
    fp = fp.set_index(keys=["accountname","stockcode"])
    fpstack = fp.stack()
    account_tmp = fpstack.reset_index()
    account_tmp = account_tmp.rename(columns={"level_2": "sym", 0: "amount"})
    account_tmp['stockcode'] = account_tmp['stockcode'].map(int_to_code)
    account_tmp['amount'] = account_tmp['amount'].map(float_to_100int)
    account_tmp['amount'] = account_tmp['amount'].astype('int')
    account = account_tmp[account_tmp['amount'] >=100]
    if(os.path.isdir(rootdir)):
        account.to_csv(rootdir + summarydir + '/distributed_stks.csv',index=False)
        acc=account[['accountname','stockcode','sym']].duplicated()
        if(acc[acc==True].index.values.size!=0):
            print('duplicated records:',account.loc[acc[acc==True].index.values])
            return None
    return account

def distribute_stks(diretory, filename, host, port):
    data = caculate_stks(diretory, filename);
    if type(data) == pd.DataFrame and not data.empty:
        print("data length: %d, data:\n" % len(data))
        print(data)
        update_data_2kdb(data, host, port)