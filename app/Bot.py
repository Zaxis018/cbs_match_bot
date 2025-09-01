from qrlib.QRBot import QRBot
from app.WeightageProcess import WeightageProcess

class Bot(QRBot):
    def __init__(self):
        super().__init__()
        self.weightage_process = WeightageProcess()

    def start(self):
        self.setup_platform_components()

        # self.weightage_process.create_table()
        self.weightage_process.before_run()
        self.weightage_process.execute_run()
        self.weightage_process.after_run()

    def teardown(self):
        # self.weightage_process.after_run()
        pass
