class Customer:
    counter = 0  # class attrs

    def __init__(self, username, password, fullname, email):
        self.username = username  # obj attrs
        self.password = password
        self.fullname = fullname
        self.email = email
        self.wallet_amount = 0
        self.is_enable = False

    def __str__(self):
        return self.username

    def check_password(self, password):
        return self.password == password

    def set_enable(self):
        self.is_enable = True


class Product:
    product_list = list()

    def __init__(self, upc, name, price=0, description=''):
        self.upc = upc
        self.name = name
        self.price = price
        self.description = description
        Product.product_list.append(self)

    def __str__(self):
        return f"upc: {self.upc}\tname: {self.name}"

    def is_free(self):
        return self.price == 0
