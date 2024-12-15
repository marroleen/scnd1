#%%билбобатеки
import pandas as pd
import numpy as np
import re
#%%сами данные
df=pd.read_csv("train.csv")
#%%функции
def fill_f2b(df):#для заполнения пропусков в старых значениях известными значениями из новых, после наоборот
    df["user_id"]=df["client_id"]
    df=df.sort_values(by=["client_id","report_date"]).groupby("user_id").ffill().bfill()
    #print(df)
    return df
def fill_b2f(df):#^только наоборот
    df["user_id"]=df["client_id"]
    df=df.sort_values(by=["client_id","report_date"]).groupby("user_id").bfill().ffill()
    #print(df)
    return df
def mt_cols(df,threshold):#убирает колонки с пропусками выше threshold(если threshold=0.999, убираем где заполнено менее .1%)
    df.drop_duplicates()
    df.dropna(how="all",axis=1,inplace=True)
    df=df.loc[:,((df==0)|(df.isnull())).mean()<threshold]
    return df
def float2int(df):#как можем переводим float в int
    float_columns=df.select_dtypes(include=np.number).columns
    for col in float_columns:
        temp=df[col].fillna(0)
        if temp.apply(lambda x:x==int(x)).all():
            df[col]=df[col].astype("Int64")
    df=df.loc[:,(df!=0).any(axis=0)]
    return df
#%%делим на 2 штуки
df_int=df.select_dtypes(include=["int64"])
df_float=df.select_dtypes(include=["float64"])
#%%инты
df_int=df_int.astype(float)
df_int=df_int.drop(["client_id","target"],axis=1)
df_int.replace("nan",np.nan,inplace=True)
def merge_columns_with_common_values(df):
    base_toggle=True
    count=0
    unique_cols=[]
    temp_col="col2"
    col_groups={}
    for col in df.columns:
        if col in unique_cols:
            continue
        if col!=df.columns[0]:
            base_toggle=False
        col_groups[col]=[col]
        for next_col in df.columns:
            if col==next_col or next_col in unique_cols:
                continue
            common_values=list(set(df[col].unique()).intersection(set(df[next_col].unique())))
            if np.nan in common_values:
                common_values.remove(np.nan)
            match1=re.match(r"([a-zA-Z]+)(\d{1,4})", next_col)
            def calc_jump():
                if base_toggle:
                    return False
                else:
                    match=re.match(r"([a-zA-Z]+)(\d{1,4})",temp_col)
                    is_jump=(int(match1.group(2))-int(match.group(2)))>5
                    return is_jump
            if (len(common_values)>=1):
                if calc_jump():
                    temp_col=next_col
                    break
                count+=1
                temp_col=next_col
                col_groups[col].append(next_col)
                unique_cols.append(next_col)
                print(f"merged {col} and {next_col}: common_values = {common_vals}")
            else:
                if calc_jump():
                    temp_col=next_col
                    break
                common_vals = list(set(df[temp_col].unique()).intersection(set(df[next_col].unique())))
                if len(common_vals)>=1:
                    count+=1
                    temp_col=next_col
                    col_groups[col].append(next_col)
                    unique_cols.append(next_col)
                    print(f"merged {col} and {next_col}: common_values = {common_vals}")
                else:break
        unique_cols.append(col)
    for group_name,group_cols in col_groups.items():
        if len(group_cols)>1:
            df[group_name]=(df[group_cols].bfill(axis=1).iloc[:, 0])
            print(f"merged {group_cols} into {group_name}")
        else:
            df[group_name]=df[group_cols[0]]
    df=df[[col for col in df.columns if col in col_groups]]
    return df,count
df_int,count=merge_columns_with_common_values(df_int)
if count==0:
    print("no merged columns?")
else:
    print(f"{count} merged.")
#%%флоуты
def merge_columns_with_common_values(df):
    base_toggle=True
    count=0
    unique_cols=[]
    temp_col="col4"
    df.replace(["nan",np.float64(0.0),np.float64(1.0)],np.nan,inplace=True)
    col_groups={}
    for col in df.columns:
        if col in unique_cols:
            continue
        if col!=df.columns[1]:
            base_toggle=False
        col_groups[col]=[col]
        for next_col in df.columns:
            if col==next_col or next_col in unique_cols:
                continue
            common_vals=list(set(df[col].unique()).intersection(set(df[next_col].unique())))
            if np.nan in common_vals:
                common_vals.remove(np.nan)
            match1=re.match(r"([a-zA-Z]+)(\d{1,4})",next_col)
            def calc_jump():
                if base_toggle==True:
                    return False
                else:
                    match=re.match(r"([a-zA-Z]+)(\d{1,4})",temp_col)
                    is_jump=((int(match1.group(2))-int(match.group(2)))>5)
                    return is_jump
            if len(common_vals)>=1:
                if calc_jump():
                    temp_col=next_col
                    break
                count+=1
                temp_col=next_col
                col_groups[col].append(next_col)
                unique_cols.append(next_col)
                print(f"merged {col} and {next_col}: common_values = {common_vals}")
            else:
                if calc_jump():
                    temp_col=next_col
                    break
                common_vals=list(set(df[temp_col].unique()).intersection(set(df[next_col].unique())))
                if len(common_vals)>=1:
                    count+=1
                    temp_col=next_col
                    col_groups[col].append(next_col) 
                    unique_cols.append(next_col) 
                    print(f"merged {col} and {next_col}: common_values = {common_vals}")
                else:
                    break
        unique_cols.append(col)
    for group_name,group_cols in col_groups.items():
        if len(group_cols)>1:
            df[group_name]=df[group_cols].bfill(axis=1).iloc[:,0]
            print(f"merged {group_cols} into {group_name}")
        else:
            df[group_name]=df[group_cols[0]]
    df=df[[col for col in df.columns if col in col_groups]]
    return df,count
df_float,count=merge_columns_with_common_values(df_float)
if count==0:
    print("no merged columns?")
else:
    print(f"{count} merged.")
#%%объединяем
df_unsorted=pd.concat([df["report_date"],df["client_id"],df["target"]],axis=1)
df_unsorted=pd.concat([df_unsorted,df_int,df_float],axis=1)
#%%сортировочка
order_dict={}
order_list=["report_date","client_id","target"]
columns=df_unsorted.columns
for column in columns:
    if "col" in column:
        order_dict[column]=int(re.match(r"([a-zA-Z]+)(\d{1,4})", column).group(2))
sorted_columns = {k:v for k,v in sorted(order_dict.items(),key=lambda item:item[1])}
for k in sorted_columns:
    order_list.append(k)
df=df_unsorted[order_list]
#%%дату в дату
df["report_date"]=pd.to_datetime(df["report_date"],format=r"%Y-%m-%d")
df.dropna(how='all',axis=1,inplace=True)
#%%сохраняем)0))
df.to_csv("train_m.csv",index=False)
fill_f2b(df).to_csv("train_f2bm.csv",index=False)
fill_b2f(df).to_csv("train_b2fm.csv",index=False)
#%%
#%%
