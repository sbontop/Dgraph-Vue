package main

import (	
	"context"
	"log"
	// ---------- DGRAPH ---------- \\
	"github.com/dgraph-io/dgo/v2"
	"github.com/dgraph-io/dgo/v2/protos/api"
	"google.golang.org/grpc"
	// ---------- PREPROCESSING ---------- \\
	"encoding/json"
    "fmt"
    "io/ioutil"
    "os"
)

// ---------- <STRUCTS> ---------- \\
// Buyers struct which contain Buyer elements
type Buyers struct {
    Buyers []Buyer `json:"buyers"`
}

// Buyer struct which contains an Id, Name and Age
type Buyer struct {
	Uid      string     `json:"uid,omitempty"`
    Id      string     `json:"buyer_id,omitempty"`
	Name    string     `json:"buyer_name,omitempty"`
	Age     int      `json:"buyer_age,omitempty"`
}

// Products struct which contain Product elements
type Products struct {
    Products []Product `json:"products"`
}

// Product struct which contains an Id, Name and Price
type Product struct {
	Uid      string     `json:"uid,omitempty"`
    Id      string  `json:"product_id"`
    Name    string  `json:"product_name"`
    Price   int  `json:"product_price"`
}

// Transactions struct which contain Transaction elements
type Transactions struct {
    Transactions []Transaction `json:"transactions"`
}

// Transaction struct which contains an Id, BuyerId, Ip, Device and Products
type Transaction struct {
	Uid		string	`json:"uid,omitempty"`
    Id      string  `json:"transaction_id,omitempty"`
    BuyerId string  `json:"buyer_id,omitempty"`
    Ip      string  `json:"ip,omitempty"`
    Device  string  `json:"device,omitempty"`
    Products []Product `json:"products,omitempty"`
}
// ---------- </STRUCTS> ---------- \\


// ---------- <FUNCTIONS> ---------- \\
func loadBuyers() *Buyers { 
    // Open our jsonFile
    jsonFile, err := os.Open("preprocess-data/files/buyers.json")
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
    jsonFile, err := os.Open("preprocess-data/files/products-processed.json")
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
    jsonFile, err := os.Open("preprocess-data/files/transactions-processed.json")
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
// ---------- </FUNCTIONS> ---------- \\

func main() {
	conn, err := grpc.Dial("localhost:9080", grpc.WithInsecure())
	if err != nil {
		log.Fatal("While trying to dial gRPC")
	}
	defer conn.Close()

	dc := api.NewDgraphClient(conn)
	dg := dgo.NewDgraphClient(dc)


	// dob := time.Date(1980, 01, 01, 23, 0, 0, 0, time.UTC)

	/*
	// Loop over buyers
	var buyers *Buyers = loadBuyers()	
    for i := 0; i < len(buyers.Buyers); i++ {
		b := Buyer {
				Uid:	"_:" + buyers.Buyers[i].Name,
				Id:		buyers.Buyers[i].Id,
				Name:	buyers.Buyers[i].Name,
				Age:	buyers.Buyers[i].Age,
		}
		op := &api.Operation{}
		op.Schema = 
		`			
			buyer_name: string @index(exact) .			
			buyer_age: string .			
			buyer_id: string .
		`

		ctx := context.Background()
		err = dg.Alter(ctx, op)
		if err != nil {
			log.Fatal(err)
		}

		mu := &api.Mutation{
			CommitNow: true,
		}
		pb, err := json.Marshal(b)
		if err != nil {
			log.Fatal(err)
		}

		mu.SetJson = pb
		assigned, err := dg.NewTxn().Mutate(ctx, mu)
		if err != nil {
			log.Fatal(err)
		}

		// Assigned uids for nodes which were created would be returned in the assigned.Uids map.
		variables := map[string]string{"$id1": assigned.Uids[buyers.Buyers[i].Name]}
		fmt.Println(variables)
		q := `query Me($id1: string){
			me(func: uid($id1)) {
				uid
				buyer_id
				buyer_name
				buyer_age
			}
		}`

		resp, err := dg.NewTxn().QueryWithVars(ctx, q, variables)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(resp.Json))

		// type Root struct {
		// 	Me []Person `json:"me"`
		// }

		// var r Root
		// err = json.Unmarshal(resp.Json, &r)
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// fmt.Printf("Me: %+v\n", r.Me)
		// R.Me would be same as the person that we set above.		
    }
	*/
	

	/*
	// Loop over products
	var products *Products = loadProducts()    
    for i := 0; i < len(products.Products); i++ {
		p := Product {
			Uid: 	"_:" + products.Products[i].Name,
			Id: 	products.Products[i].Id,
			Name: 	products.Products[i].Name,
			Price: 	products.Products[i].Price,
		}

		op := &api.Operation{}
		op.Schema = 
		`
			product_name: string @index(exact) .			
			product_price: int .			
			product_id: string .
		`

		ctx := context.Background()
		err = dg.Alter(ctx, op)
		if err != nil {
			log.Fatal(err)
		}

		mu := &api.Mutation{
			CommitNow: true,
		}
		pb, err := json.Marshal(p)
		if err != nil {
			log.Fatal(err)
		}

		mu.SetJson = pb
		assigned, err := dg.NewTxn().Mutate(ctx, mu)
		if err != nil {
			log.Fatal(err)
		}

		// Assigned uids for nodes which were created would be returned in the assigned.Uids map.
		variables := map[string]string{"$id1": assigned.Uids[products.Products[i].Name]}
		fmt.Println(variables)
		q := `query Me($id1: string){
			me(func: uid($id1)) {
				uid
				product_id
				product_name
				product_price
			}
		}`

		resp, err := dg.NewTxn().QueryWithVars(ctx, q, variables)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(resp.Json))
    }
	*/
	
	// Loop over transactions
	var transactions *Transactions = loadTransactions()    
    for i := 0; i < len(transactions.Transactions); i++ {
        fmt.Println("Id: " + transactions.Transactions[i].Id)
        fmt.Println("BuyerId: " + transactions.Transactions[i].BuyerId)
        fmt.Println("Ip: " + transactions.Transactions[i].Ip)
        fmt.Println("Device: " + transactions.Transactions[i].Device)

		for j := 0; j < len(transactions.Transactions[i].Products); j++ {
			fmt.Println("Product Id:" + transactions.Transactions[i].Products[j].Id)

			t := Transaction {
				Uid:	"_:" + transactions.Transactions[i].Id,
				Id:		transactions.Transactions[i].Id,
				Ip: 	transactions.Transactions[i].Ip,
				Device: transactions.Transactions[i].Device,
				Products:	[]Product{{
					Id:	transactions.Transactions[i].Products[j].Id,
				}},
				
			}
	
			op := &api.Operation{}
			op.Schema = 
			`
				transaction_id: string @index(exact) .			
				buyer_id:	string .			
				ip:			string .
				device:		string .
				products: 	[]Product .
			`
	
			ctx := context.Background()
			err = dg.Alter(ctx, op)
			if err != nil {
				log.Fatal(err)
			}
	
			mu := &api.Mutation{
				CommitNow: true,
			}
			pb, err := json.Marshal(t)
			if err != nil {
				log.Fatal(err)
			}
	
			mu.SetJson = pb
			assigned, err := dg.NewTxn().Mutate(ctx, mu)
			if err != nil {
				log.Fatal(err)
			}
	
			// Assigned uids for nodes which were created would be returned in the assigned.Uids map.
			variables := map[string]string{"$id1": assigned.Uids[transactions.Transactions[i].Id]}
			fmt.Println(variables)
			q := `query Me($id1: string){
				me(func: uid($id1)) {
					uid
					transaction_id
					buyer_id
					ip
					device
					products {
								product_id
					}
				}
			}`
	
			resp, err := dg.NewTxn().QueryWithVars(ctx, q, variables)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println(string(resp.Json))
		}		
    }

}
