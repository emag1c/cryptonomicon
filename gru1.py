from torch.utils.data import Dataset, random_split, DataLoader
import torch
from collections import OrderedDict
import pandas as pd
import numpy as np
import pytorch_lightning as pl
from typing import List
import math


class ChartDataset(Dataset):
    """ DataSet for charts """

    def __init__(self, csv_files: List[str], window: int, target_col: str):
        self._window = window
        self._row_cnt = 0
        self._slices = OrderedDict()
        self._target_col = target_col

        for csv_file in csv_files:
            with open(csv_file) as cfile:
                row_cnt = sum(1 for l in cfile) - 1
                self._row_cnt += row_cnt

            _first_idx = len(self._slices)
            for i in range(_first_idx, row_cnt + _first_idx):
                if i + self._window > self._row_cnt:
                    break
                self._slices[i] = {
                    'file': csv_file,
                    'data': None,
                }

        self._len = len(self._slices)

        assert(self._len > 0), "length of chart slices must be greater than 0"

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
        """
        :param idx:
        :return: tuple (2d data tensor, target)
        """
        df = self.load_slice(idx)
        # target is the last value in the window
        target = df[self._target_col].iloc[-1]
        return torch.tensor(df.values), target


class ExtendedCandleDataModule(pl.LightningDataModule):
    def __init__(self, csv_files: List[str], window: int, target_col: str = "target", batch_size: int = 32):
        super().__init__()
        self.batch_size = batch_size
        self.target_col = target_col
        self.window_size = window
        self.csv_files = csv_files
        self.train = None
        self.valid = None
        self.test = None

        self.dims = None

    def prepare(self):
        # todo: possibly apply normalization, etc here
        pass

    def setup(self, stage=None):
        fullset = ChartDataset(self.csv_files, window=self.window_size, target_col=self.target_col)
        # 60/40 split
        fullset_len = len(fullset)
        train_size = math.ceil(fullset_len * .55)
        valid_size = math.ceil(fullset_len * .35)
        test_size = len(fullset) - (train_size + valid_size)
        self.train, self.valid, self.test = random_split(fullset, [train_size, valid_size, test_size])

        self.dims = self.train[0][0].shape

    def train_dataloader(self, *args, **kwargs) -> DataLoader:
        return DataLoader(self.train, batch_size=self.batch_size, shuffle=True)

    def val_dataloader(self, *args, **kwargs) -> DataLoader:
        return DataLoader(self.valid, batch_size=self.batch_size, shuffle=True)

    def test_dataloader(self, *args, **kwargs) -> DataLoader:
        return DataLoader(self.test, batch_size=self.batch_size, shuffle=True)


if __name__ == '__main__':
    # todo: split dataset into validation and test
    dataset = ChartDataset(["data/xtbusd_1h_2019.csv"], 200, False)

    # module = ExtendedCandleDataModule(["data/xtbusd_1h_2019.csv"])



