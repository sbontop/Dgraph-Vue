package loadData

import (
    // readjson libraries
    "encoding/json"
    "fmt"
    "io/ioutil"
    "os"
    // "strings"
    // "strconv"
)

// Buyers struct which contain Buyer elements
type Buyers struct {
    Buyers []Buyer `json:"buyers"`
}

// Buyer struct which contains an Id, Name and Age
type Buyer struct {
    Id      string     `json:"id,omitempty"`
	Name    string     `json:"name,omitempty"`
	Age     int        `json:"age,omitempty"`
}

// Products struct which contain Product elements
type Products struct {
    Products []Product `json:"products"`
}

// Product struct which contains an Id, Name and Price
type Product struct {
    Id      string  `json:"product_id"`
    Name    string  `json:"product_name"`
    Price   string  `json:"product_price"`
}

// Transactions struct which contain Transaction elements
type Transactions struct {
    Transactions []Transaction `json:"transactions"`
}

// Transaction struct which contains an Id, BuyerId, Ip, Device and Products
type Transaction struct {
    Id      string  `json:"id,omitempty"`
    BuyerId string  `json:"buyer_id,omitempty"`
    Ip      string  `json:"ip,omitempty"`
    Device  string  `json:"device,omitempty"`
    ProductIds []string `json:"product_ids,omitempty"`
}

func main() {
    
    // var buyers *Buyers = loadBuyers()
    // // Loop over buyers
    // for i := 0; i < len(buyers.Buyers); i++ {
    //     fmt.Println("Id: " + buyers.Buyers[i].Id)
    //     fmt.Println("Name: " + buyers.Buyers[i].Name)
    //     fmt.Println("Age: " + strconv.Itoa(buyers.Buyers[i].Age))
    // }    

    // var products *Products = loadProducts()
    // // Loop over products
    // for i := 0; i < len(products.Products); i++ {
    //     fmt.Println("Id: " + products.Products[i].Id)
    //     fmt.Println("Name: " + products.Products[i].Name)
    //     fmt.Println("Price: " + products.Products[i].Price)
    // }

    // var transactions *Transactions = loadTransactions()
    // // Loop over transactions
    // for i := 0; i < len(transactions.Transactions); i++ {
    //     fmt.Println("Id: " + transactions.Transactions[i].Id)
    //     fmt.Println("BuyerId: " + transactions.Transactions[i].BuyerId)
    //     fmt.Println("Ip: " + transactions.Transactions[i].Ip)
    //     fmt.Println("Device: " + transactions.Transactions[i].Device)
    //     fmt.Println(transactions.Transactions[i].ProductIds)
    // }
}


func loadBuyers() *Buyers { 
    // Open our jsonFile
    jsonFile, err := os.Open("files/buyers.json")
    // if we os.Open returns an error then handle it
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println("Successfully Opened buyers.json")
    // defer the closing of our jsonFile so that we can parse it later on
    defer jsonFile.Close()

    // read our opened xmlFile as a byte array.
    byteValue, _ := ioutil.ReadAll(jsonFile)

    // we initialize our Buyers array
    var buyers Buyers

    // we unmarshal our byteArray which contains our
    // jsonFile's content into 'buyers' which we defined above
    json.Unmarshal(byteValue, &buyers)

    return &buyers
}

func loadProducts() *Products {  
    // Open our jsonFile
    jsonFile, err := os.Open("files/products-processed.json")
    // if we os.Open returns an error then handle it
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println("Successfully Opened products-processed.json")
    // defer the closing of our jsonFile so that we can parse it later on
    defer jsonFile.Close()

    // read our opened xmlFile as a byte array.
    byteValue, _ := ioutil.ReadAll(jsonFile)

    // we initialize our Buyers array
    var products Products

    // we unmarshal our byteArray which contains our
    // jsonFile's content into 'products' which we defined above
    json.Unmarshal(byteValue, &products)

    return &products
}

func loadTransactions() *Transactions {
    // Open our jsonFile
    jsonFile, err := os.Open("files/transactions-processed.json")
    // if we os.Open returns an error then handle it
    if err != nil {
        fmt.Println(err)
    }

    fmt.Println("Successfully Opened transactions-processed.json")
    // defer the closing of our jsonFile so that we can parse it later on
    defer jsonFile.Close()

    // read our opened xmlFile as a byte array.
    byteValue, _ := ioutil.ReadAll(jsonFile)

    // we initialize our Transactions array
    var transactions Transactions

    // we unmarshal our byteArray which contains our
    // jsonFile's content into 'transactions' which we defined above
    json.Unmarshal(byteValue, &transactions)

    return &transactions
}