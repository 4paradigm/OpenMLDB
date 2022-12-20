import pandas as pd
import xxhash
import numpy as np
import tritonclient.http as httpclient

cols = ['C1','C2','C3','C4','C5','C6','I1','I2','I3','I4','I5','C7','C8','C9','Label','C10','C11','C12','C13','C14','C15','C16','C17','C18','C19','C20','I6','I7','I8','I9','I10','C21','C22','C23','C24','C25','C26','I11','I12','I13','C27','C28']

res_cols = ['Label',
        'I1', 'I2', 'I3', 'I4', 'I5', 'I6', 'I7', 'I8', 'I9', 'I10', 'I11', 'I12', 'I13',
        'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7','C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14',
        'C15', 'C16', 'C17', 'C18', 'C19', 'C20', 'C21', 'C22','C23', 'C24', 'C25', 'C26', 'C27', 'C28']

def get_schema():
    dict_schema_tmp = {}
    for i in table_schema:
        dict_schema_tmp[i[0]] = i[1]
    return dict_schema_tmp

def generate_hash(val):
    res = []
    if val.name == 'Label':
        return val
    for i in val:
        test = xxhash.xxh64(str(i), seed = 10)
        res.append(test.intdigest())
    return res

def rearrange_and_mod(indata):
    import pdb; pdb.set_trace()
    if (len(cols) != len(indata) ):
        self.write("Sample length not equal, please check")
        return None

    df = pd.DataFrame(columns=cols)
    df.loc[0]=val
    df = df[res_cols]

    df= df.apply(lambda x: generate_hash(x), axis = 0)

    for col in df.columns:
        if col == 'Label':
            df[col] = df[col].astype('float32')
        else:
            df[col] = df[col].astype('int64')
    return df


def process_res(datadir):
    data= pd.read_csv(datadir)
    import pdb; pdb.set_trace()
    res = rearrange_and_mod(data)
    print(res)

def oneflow_infer(url, data):
    triton_client = httpclient.InferenceServerClient(url=url)
    inputs = []
    inputs.append(httpclient.InferInput('INPUT_0', data.shape, "INT64"))
    inputs[0].set_data_from_numpy(data, binary_data=True)
    outputs = []
    outputs.append(httpclient.InferRequestedOutput('OUTPUT_0', binary_data=True, class_count=1))
    results = triton_client.infer("embedding", inputs=inputs, outputs=outputs)
    output_data = results.as_numpy('OUTPUT_0')
    return output_data


def process_infer(url, data):

    df = pd.DataFrame(columns=cols)
    df.loc[0]=data
    df = df[res_cols]

    df= df.apply(lambda x: generate_hash(x), axis = 0)

    for col in df.columns:
        if col == 'Label':
            df[col] = df[col].astype('float32')
        else:
            df[col] = df[col].astype('int64')

    label = df['Label']
    del df['Label']
    data = df.values
    res = oneflow_infer(url, data)
    return res
