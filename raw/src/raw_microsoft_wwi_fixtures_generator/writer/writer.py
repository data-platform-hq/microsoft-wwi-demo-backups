from raw_microsoft_wwi.configuration import Config


class WriterCSV:
    def __init__(self, config: Config, limit):
        self.config = config
        self.first_run = True
        self.limit = limit

    def write_table(self, table_data, table_name, sort_field):
        table_data.coalesce(1).orderBy(sort_field).limit(self.limit).write.csv(
            f'{self.config.input_delta_tables_path}/{table_name.replace("_", "/")}',
            mode="overwrite", header=True, sep="\t")
