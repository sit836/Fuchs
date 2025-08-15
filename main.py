import pandas as pd

PRODUCTS = ['400328186', '400328193', '400328216', '400330356', '400353867', '400360247', '400370338']
PROD_RECIPE_VER_MAP = {'400328186': [12, 13, 14, 15],
                       '400328193': [11, 12, 13],
                       '400328216': [8],
                       '400330356': [2],
                       '400353867': [6, 7, 8, 10],
                       '400360247': [6, 7],
                       '400370338': [1, 3],
                       }


def create_prod_info_dict():
    prod_info_dict = {}
    for product in PRODUCTS:
        df_1 = pd.read_excel('./data/表1 批次信息.xlsx', sheet_name=product)
        prod_info_dict[product] = df_1.groupby('配方版本号')['产品批次号'].unique().to_dict()
    return prod_info_dict


def read_process_table4(prod_info_dict):
    df_4 = pd.read_excel('./data/表4 成品检测结果.xlsx')
    grouped = df_4.groupby(['原料/产品 名称', '原料/产品 批次'])['检测项目名称'].apply(set)
    common_test_items = set.intersection(*grouped).difference({'CNAPP001'})

    records = []
    for product in PRODUCTS:
        recipe_vers = PROD_RECIPE_VER_MAP[product]
        for recipe_ver in recipe_vers:
            batch_ids = prod_info_dict[product][recipe_ver]

            grouped = df_4.loc[
                df_4['原料/产品 批次'].isin(batch_ids) & df_4['检测项目名称'].isin(common_test_items), ['检测项目名称',
                                                                                                        '检测结果']]
            grouped['产品'] = product
            grouped['配方版本'] = recipe_ver

            records.append(grouped)

    result_df = pd.concat(records)

    pivot_df = result_df.pivot_table(
        index=['产品', '配方版本'],
        columns='检测项目名称',
        values='检测结果'
    ).reset_index()
    return pivot_df


def read_process_table2():
    df_raw = pd.read_excel('./data/表2 配方信息.xlsx', header=None)
    df_raw = df_raw.T.fillna(0)
    df_raw.columns = df_raw.iloc[0]
    return df_raw.drop(0).reset_index(drop=True).astype({'产品': int, '配方版本': int}).astype({'产品': str})


prod_info_dict = create_prod_info_dict()
df_Y = read_process_table4(prod_info_dict)
df_X = read_process_table2()
df_XY = df_X.merge(df_Y, on=['产品', '配方版本'])

df_XY['辅助变量'] = 1.

df_XY.to_csv('data.csv', index=False)
