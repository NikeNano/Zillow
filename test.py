# Filename: run_luigi.py
import luigi
import numpy as np 
import numpy as np
import pandas as pd

# only run if there are any missing dependencies! 
 
class PrintNumbers(luigi.Task):
 
    def requires(self):
        return []
 
    def output(self):
        return luigi.LocalTarget("numbers_up_to_11.txt")
 
    def run(self):
        self.readData()
        with self.output().open('w') as f:
            for i in range(1, 12):
                f.write("{}\n".format(i))
    
    def readData(self):
        path_data = '/Users/Niklas/Documents/Zillow_data/properties_2016.csv'
        self.dataframe=pd.read_csv(path_data)

    def removeDataWhenTolittle(self):
        a=self.dataframe.describe(include='all')
        a=a.loc["count",:]
        drop=[]
        for ind, val in a.iteritems():
            print(ind,val)
            if val<2900000:
                drop.append(ind)
        dataframe_reduced = dataframe.drop(drop, 1)

class SquaredNumbers(luigi.Task):
 
    def requires(self):
        return [PrintNumbers()]
 
    def output(self):
        return luigi.LocalTarget("squares.txt")
 
    def run(self):
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out))

class CubicNUmbers(luigi.Task):
    def requires(self):
        return [PrintNumbers()]
    
    def output(self):
        return luigi.LocalTarget("cubic.txt")

    def run(self):
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for line in fin:
                n = int(line.strip())
                out = n * n
                fout.write("{}:{}\n".format(n, out)) 
           

class HigherNumbers(luigi.Task):
    def requires(self):
        return  [PrintNumbers(), CubicNUmbers()]               

    def output(self):
        return luigi.LocalTarget("new.txt")

    def run(self):
        with self.output().open('w') as fout:
            fout.write("hej") 

if __name__ == '__main__':
    luigi.run()
