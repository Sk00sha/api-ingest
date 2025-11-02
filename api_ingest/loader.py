class Loader:
    def __init__(self, spark):
        self.spark = spark

    def write_csv(self, df, output_path):
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"✅ CSV written to: {output_path}")
