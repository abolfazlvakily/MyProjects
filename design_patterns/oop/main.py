from design_patterns.oop.models import Customer, Product

if __name__ == "__main__":
    # Create customers
    c1 = Customer('hosein', '123', 'Hosein ', 'hosein@mail.com')
    c2 = Customer('ali', '456', 'Ali ', 'ali@mail.com')
    c3 = Customer(
        email='reza@mail.com', fullname='Reza', password='789', username='reza'
    )

    c1.set_enable()
    # print("Check password: ", c1.check_password('123'))
    print(c1)

    # Create Products
    p1 = Product(1, 'Product #1')
    p2 = Product(2, 'Product #2', 1000)
    p3 = Product(3, 'Product #3', 1000, 'Some description about product')

    # print(p1.upc, p1.is_free())
    # print(p2.upc, p2.is_free())
    # print(p3.upc, p3.is_free())
    for pr in Product.product_list:
        print(pr)
