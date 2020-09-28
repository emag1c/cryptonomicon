from torch.utils.data import Dataset
from collections import OrderedDict
import pandas as pd
import numpy as np
from fastai.data.block import DataLoader
from fastai.data.block import DataBlock


class ExtendedCandleDataset(Dataset):
    """ DataSet for extended Candles """

    def __init__(self, csv_files: [], window: int, is_valid: bool):
        self._window = window
        self._row_cnt = 0
        self._slices = OrderedDict()
        self.is_valid = is_valid

        for csv_file in csv_files:
            with open(csv_file) as cfile:
                _row_cnt = sum(1 for l in cfile) - 1
                self._row_cnt += _row_cnt

            _first_idx = len(self._slices)
            for _i in range(_first_idx, _row_cnt + _first_idx):
                if _i + self._window > self._row_cnt:
                    break
                self._slices[_i] = {
                    'file': csv_file,
                    'data': None,
                }

        self._len = len(self._slices)

        first_df = pd.read_csv(self._slices[0]['file'], header=0, nrows=1)
        self._columns = first_df.columns

    def load_slice(self, idx: int) -> pd.DataFrame:
        return pd.read_csv(self._slices[idx]['file'],
                           header=None,
                           names=self._columns,
                           skiprows=idx+1,
                           nrows=self._window,
                           dtype=np.float64)

    def __len__(self):
        return len(self._slices)

    def __getitem__(self, idx):
        if self.is_valid:
            # if validation set then pop off the validation columns
            # todo: pop off valid column
            pass
        return self.load_slice(idx).to_numpy(dtype=np.float64)


if __name__ == '__main__':
    # todo: split dataset into validation and test
    dataset = ExtendedCandleDataset(["data/xtbusd_1h_2019.csv"], 200, False)

    dl = DataLoader(dataset=dataset, bs=4, shuffle=True)



