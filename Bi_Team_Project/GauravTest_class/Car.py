
class Car:
    def __init__(self, make, model):
        self.model = model
        self.make = make

    def printCarData(self):
        print(self.make)
        print(self.model)