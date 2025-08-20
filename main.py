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
    result = []
    for product in PRODUCTS:
        df_1 = pd.read_excel('./data/表1 批次信息.xlsx', sheet_name=product)
        result.append(df_1)
        prod_info_dict[product] = df_1.groupby('配方版本号')['产品批次号'].unique().to_dict()
    result_df = pd.concat(result).rename(columns={'产品名称': '产品', '配方版本号': '配方版本', '产品批次号': '产品批次', '原料批次号': '原料批次'})
    result_df = result_df[['产品', '配方版本', '产品批次', '原料批次', '原料名称', '原料权重']]
    return prod_info_dict, result_df


def read_process_table3():
    df_3 = pd.read_excel('./data/表3 原料检测结果.xlsx')
    material_features = df_3.groupby(['原料名称', '原料批次', '检测项目名称'])['检测结果'].first()
    result_df = material_features.unstack(level=['原料名称', '检测项目名称']).reset_index()
    material_cols = [f'{batch}_{item}' for batch, item in result_df.columns[1:]]
    result_df.columns = ['原料批次'] + material_cols
    return result_df, material_cols


def read_process_table4(prod_info_dict):
    df_4 = pd.read_excel('./data/表4 成品检测结果.xlsx')
    grouped = df_4.groupby(['原料/产品 名称', '原料/产品 批次'])['检测项目名称'].apply(set)
    common_test_items = set.intersection(*grouped).difference({'CNAPP001'})

    records = []
    for product in PRODUCTS:
        recipe_vers = PROD_RECIPE_VER_MAP[product]
        for recipe_ver in recipe_vers:
            batch_ids = prod_info_dict[product][recipe_ver]

            grouped = (
                df_4[df_4['原料/产品 批次'].isin(batch_ids)]
                .groupby(['原料/产品 批次', '检测项目名称'])['检测结果']
                .mean()
                .reset_index()
            )
            is_selected = grouped['检测项目名称'].isin(common_test_items)
            grouped = grouped[is_selected]

            grouped['产品'] = int(product)
            grouped['配方版本'] = int(recipe_ver)

            records.append(grouped)

    result_df = pd.concat(records)
    pivot_df = result_df.pivot_table(
        index=['产品', '配方版本', '原料/产品 批次'],
        columns='检测项目名称',
        values='检测结果'
    ).reset_index().rename(columns={'原料/产品 批次': '产品批次'})
    pivot_df.columns.name = None
    return pivot_df


def read_process_table2():
    df_raw = pd.read_excel('./data/表2 配方信息.xlsx', header=None)
    df_raw = df_raw.T
    df_raw.columns = df_raw.iloc[0]
    df_raw = df_raw.drop(0).reset_index(drop=True)
    df_raw['产品'] = df_raw['产品'].ffill()
    df_raw = df_raw.fillna(0)
    return df_raw.astype({'产品': int, '配方版本号': int}).rename(columns={'配方版本号': '配方版本'})


prod_info_dict, df_batch_info = create_prod_info_dict()
df_prod_test = read_process_table4(prod_info_dict)
df_prod_recipe = read_process_table2()
df_material_test, material_test_cols = read_process_table3()

df = df_batch_info.merge(df_material_test, on='原料批次').drop(columns=['原料批次']).fillna(0)
df[material_test_cols] = df[material_test_cols] * df['原料权重'].values[:, None]
df = df.groupby(['产品', '配方版本', '产品批次'])[material_test_cols].sum().reset_index()
df = df.merge(df_prod_recipe, on=['产品', '配方版本'])
df = df.merge(df_prod_test, on=['产品', '配方版本', '产品批次'], how='left')

df['辅助变量'] = 1.

df = df[df.columns[df.sum(axis=0) != 0]]
df.to_csv('data.csv', index=False)
