import glob
import pandas as pd
import os

src = r'C:\Users\Jared\Downloads\jlawren2\jlawren2\home11\jlawren2\Valorant Projects\clean'
files = glob.iglob(os.path.join(src, "*.csv"))
df_from_each_file = (pd.read_csv(f).assign(timestamp=os.path.basename(f).split('.')[0]) for f in files)
csv_data = pd.concat(df_from_each_file, ignore_index=True)
csv_data.to_csv('complete.csv')

