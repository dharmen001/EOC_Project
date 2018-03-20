
import Car

class Service(Car):
    def __init__(self, make, model, serviceyear):
        super.__init__(make, model)
        self.serviceyear = serviceyear

    def printServiceData(self):
        print(self.serviceyear)


if __name__ == "__main__":
    o = Service(2017, 'Ford', 2018)
    o.printServiceData()
