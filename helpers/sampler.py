# instead of loading and treating 15 Million Rows, we extract a sample of
# 1000 random rows to do data exploration


class Sampler():
    @staticmethod
    def taxi_data_sampling(dataset,sampling_path):
        taxi_smapled_data = dataset.sample(False,0.1,0.45).limit(1000)
        taxi_smapled_data = (taxi_smapled_data.toPandas())
        taxi_smapled_data.to_excel(sampling_path)