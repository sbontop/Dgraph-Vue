
import json

"""
Read products in json file and edit json fields
"""
# def preprocessBuyers(filename):


"""
Read products in csv file and rewrite it in json format
"""


def preprocessProducts(filename):
    productsFile = open(filename, "r+")
    data = {}
    data['products'] = []
    no_line = 0
    for line in productsFile:
        # remove double quotes from product name
        if '\"' in line:
            line = line.replace("\"", "")

            for idx in range(len(line) - 1):
                letter = line[idx]
                # when finds simple quote among two lowercase characters, should be removed
                validation = (letter == '\'') and (
                    line[idx-1].isalpha() and line[idx-1].islower()) and (line[idx+1].isalpha() and line[idx+1].islower())
                if validation:
                    line = line[:idx] + line[idx+1:]
        line = line.strip().split('\'')
        # skip first line cause is headers
        if no_line > 0:
            # print(line)
            product_id = line[0]
            product_name = line[1]
            product_price = int(line[2])

            data['products'].append({
                'product_id': product_id,
                'product_name': product_name,
                'product_price': product_price
            })
        no_line += 1
    with open("files/products-processed.json", "w+") as outfile:
        json.dump(data, outfile)
    outfile.close()


"""
Read transactions in csv file and rewrite it in json format
"""

def getProductArray(arr_product_ids):
    arr = []
    for product_id in arr_product_ids:
        arr.append({
            'product_id': product_id
        })
    return arr

def preprocessTransactions(filename):
    transactionsFile = open(filename, "r+")
    data = {}
    data['transactions'] = []
    for line in transactionsFile:
        line = line.split('#')
        no_line = 0
        # print(line)
        for element in line:
            # skip first line cause is empty
            if no_line > 0:
                # print(element)
                # print(element.split(','))
                transaction = element.split('_')

                transaction_id = transaction[0]
                buyer_id = transaction[1]
                ip = transaction[2]
                device = transaction[3]

                product_ids = transaction[4]
                product_ids = product_ids.replace("(", "")
                product_ids = product_ids.replace(")", "")
                product_ids = product_ids.split(',')

                # data['transactions']['products'] = []
                # for product_id in product_ids:
                #     data['transactions']['products'].append({
                #         'product_id': product_id
                #     })

                data['transactions'].append({
                    'transaction_id': transaction_id,
                    'buyer_id': buyer_id,
                    'ip': ip,
                    'device': device,
                    'products': getProductArray(product_ids),
                })
            no_line += 1
    with open("files/transactions-processed.json", "w+") as outfile:
        json.dump(data, outfile)
    outfile.close()


# Call methods
# preprocessProducts("files/products.csv")
preprocessTransactions("files/transactions.csv")
