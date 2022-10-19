"""
Copyright 2020 The OneFlow Authors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import time
import numpy as np
import tritonclient.http as httpclient


if __name__ == '__main__':
    triton_client = httpclient.InferenceServerClient(url='127.0.0.1:8000')

    data = np.ones((1,41)).astype(np.int64)

    inputs = []
    inputs.append(httpclient.InferInput('INPUT_0', data.shape, "INT64"))
    inputs[0].set_data_from_numpy(data, binary_data=True)
    outputs = []
    outputs.append(httpclient.InferRequestedOutput('OUTPUT_0', binary_data=True, class_count=1))
    now = time.time()
    results = triton_client.infer("embedding", inputs=inputs, outputs=outputs)
    print(time.time() - now)
    output_data0 = results.as_numpy('OUTPUT_0')
    print(output_data0.shape)
    print(output_data0)
