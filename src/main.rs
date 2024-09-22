#![allow(warnings)]


/*
    *Правки
    Поиск по Имени и Фамилии используя фильтр по возрасту
    Презентация проекта
*/



use log::*;
use tokio::time::Instant;
use serde_json::*;
use reqwest::*;
use pyo3::*;
use env_logger::*;
use chrono::*;
use sqlx::MySqlPool;



const VK_ACCESS_TOKEN: &str = "access_token=vk1.a.WlS2F2bOSTW8J32o6ZYLjZF8zlEIsNWPYMyf9mYLRz-w5bJw_9JBTCkSEUwWVCQKE9k3yfPwwejLHlkua0-W3uH3Rz7aOcKjZAuveHguxhRhm0SFWVasCz5ZMMa2VJYnQCjeOhmNEMyFBb6WJRjfgNXw_dDFjhXus9bLHtyoWQGMmFfhbdvWEQ42mk7I8zzGj4aeV242KnxnhKl-MgxMDg";
const VK_BASE_API_URL: &str = "https://api.vk.com/method/";
const VK_API_VERSION: &str = "v=5.131";


struct VkAPI {
    url: String,
    client: Client,
}


fn string_to_json(text: String) -> serde_json::Value {
    serde_json::from_str::<Value>(&text).unwrap()
}


//+
fn parse_group_info(text: serde_json::Value) -> Vec<String> {
    let a = text.get("response").unwrap();
    let b = a.as_array().unwrap();

    let mut v = vec![];

    for i in b {
        v.push(i.get("name").unwrap().as_str().unwrap().to_string());
        v.push(i.get("description").unwrap().as_str().unwrap().to_string());        
    }

    info!("{:?}", v);
    v
}


// -
fn parse_main_info(text: serde_json::Value) -> Vec<String> {
    info!("{}", text);
    vec![]
}

// -
fn parse_all_photo(text: serde_json::Value) -> Vec<String> {
    let a = text.get("response").unwrap();
    info!("{}", a);
    vec![]
}

// -
fn parse_photo(text: serde_json::Value) -> Vec<String> {

    let a = text.get("response").unwrap();
    let b = a.get("items").unwrap();
    let c = b.as_array().unwrap();


    let mut v: Vec<String> = vec![];

    /*

        date
        id
        url

    */


    for i in c {
        println!("{:?}", i);
    }


    vec![]
}


// +
fn parse_name(text: serde_json::Value) -> Vec<String>{
    let a = text.get("response").unwrap();
    let b = a.as_array().unwrap();

    let mut first_name = String::new();
    let mut last_name = String::new();
    let mut id: String = String::new();

    for i in b {

        if let Some(fname) = i.get("first_name") {
            first_name = fname.as_str().unwrap().to_string();
        } else { first_name = "None".into() }

        if let Some(lname) = i.get("last_name") {
            last_name = lname.as_str().unwrap().to_string();
        } else { last_name = "None".into() }
        

        if let Some(_id) = i.get("id") {
            id = _id.to_string();
        }

    }

    vec![first_name, last_name, id]
}


//+
fn parse_group_id(text: serde_json::Value) -> Vec<String> {

    let a = text.get("response").unwrap();
    let b = a.get("groups").unwrap();
    let c = b.get("items").unwrap();
    let d = c.as_array().unwrap();

    let mut v = vec![];

    for i in d {
        v.push(i.as_u64().unwrap().to_string());
    }

    info!("{:?}", v);

    v
}


// +
fn parse_friends(text: serde_json::Value) -> Vec<String> {
    
    let a = text.get("response").unwrap();
    let b = a.get("items").unwrap();
    let c = b.as_array().unwrap();

    let mut first_name = String::new();
    let mut last_name = String::new();
    let mut id: String = String::new();

    let mut friend: Vec<String> = vec![];

    for i in c {

        if let Some(fname) = i.get("first_name") {
            friend.push(fname.as_str().unwrap().to_string());
        } else { first_name = "None".into() }

        if let Some(lname) = i.get("last_name") {
            friend.push(lname.as_str().unwrap().to_string());
        } else { last_name = "None".into() }
        

        if let Some(_id) = i.get("id") {
            friend.push(_id.to_string());
        }

    }

    friend
}


// +
fn parse_date(text: serde_json::Value) -> Vec<String> {

    let a = text.get("response").unwrap();
    let b = a.as_array().unwrap();
    let mut t: bool = true;
    let mut v: Vec<String> = vec![];

    /*
        date
        age
    */

    for i in b {
        if let Some(date) = i.get("bdate") {

            let d = date.as_str().unwrap().to_string().clone();
            v.push(d);

            let now_year = chrono::Local::now().year();
            let a: Vec<&str> = date.as_str().unwrap().split(".").collect::<_>();
            let age = now_year - a.last().unwrap().parse::<i32>().unwrap();
            v.push(age.to_string());

            t = false;
        }
    }

    if t { v.push("None".to_string()) }

    v
}   

// -
fn parse_post(text: serde_json::Value) -> Vec<String> {
    info!("{}", text);
    let a = text.get("response").unwrap();
    
    let b = a.get("items").unwrap();
    let count = a.get("count").unwrap().as_u64().unwrap();

    info!("{}", count);


    /*
        count
        type
        text
        date
    */

    for i in b.as_array() {
        for k in i {
            let time = k.get("date").unwrap().as_u64().unwrap();
            info!("{:?}", k.get("post_type"));
            info!("{:?}", k.get("text"));
            let native = chrono::NaiveDateTime::from_timestamp(time as i64, 0);
            println!("VK Date: {}", native);
        }
    }   

    vec![]
}

// 457239017

impl VkAPI {

    fn new() -> Self {
        let client: Client = Client::new();
        let url: String = String::from(VK_BASE_API_URL);

        Self {
            url,
            client
        }
    }

    // +
    async fn user_get_date(&self, id: String) -> Option<serde_json::Value> {

        let url = VK_BASE_API_URL.to_owned() 
                        + "users.get?" 
                        + &format!("user_id={id}") 
                        + &format!("&fields=bdate")
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");


        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }               
    }

    // -
    async fn user_get_profile_photos(&self, id: String) -> Option<serde_json::Value> {


        let url = VK_BASE_API_URL.to_owned() 
                        + "photos.get?" 
                        + &format!("user_id={id}") 
                        + &format!("&album_id=profile")
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");


        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }

    }

    // +
    async fn user_get(&self, id: String) -> Option<serde_json::Value> {

        let url = VK_BASE_API_URL.to_owned() 
                        + "users.get?" 
                        + &format!("user_id={id}") 
                        + &format!("&fields=first_name,last_name")
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }

    // -
    async fn user_get_posts(&self, id: String) -> Option<serde_json::Value> {

        let url = format!(
            "{}wall.get?owner_id=-{}&count=100&{}&{}",
            VK_BASE_API_URL, id, VK_ACCESS_TOKEN, VK_API_VERSION
        );

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }


    async fn users_get_id(&self) -> Option<serde_json::Value> {
        //"{}users.search?q=&count=200&access_token={}&v={}",
        let url = VK_BASE_API_URL.to_owned() 
                        + "users.searchq=?" 
                        + &format!("&count=200") 
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;
        let resp = resp.unwrap();
        


        None
    }


    async fn user_get_all_photos(&self, id: String) -> Option<serde_json::Value> {
        // "https://api.vk.com/method/photos.getAll?owner_id={}&access_token={}&v=5.131"

        let url = VK_BASE_API_URL.to_owned() 
                        + "photos.getAll?" 
                        + &format!("user_id={id}") 
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }


    async fn user_get_group_info(&self, id: String)  -> Option<serde_json::Value> {
        // groups.getById
        // groups.getById?group_id={}&amp;access_token={}&amp;v={}

        let url = format!(
            "{}groups.getById?group_ids={}&{}&{}&fields=description",
            VK_BASE_API_URL, id, VK_ACCESS_TOKEN, VK_API_VERSION, 
        );

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }


    async fn user_get_group_id(&self, id: String) -> Option<serde_json::Value> {

        let url = format!(
            "{}users.getSubscriptions?user_id={}&{}&{}",
            VK_BASE_API_URL, id, VK_ACCESS_TOKEN, VK_API_VERSION
        );

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }


    // 
    async fn user_get_main_info(&self, id: String) -> Option<serde_json::Value> {
        // https://api.vk.com/method/users.get?user_ids={}&fields=city,education,career&access_token={}&v=5.131

        let url = format!(
            "{}users.get?user_id={}&fields=city,education,career&{}&{}",
            VK_BASE_API_URL, id, VK_ACCESS_TOKEN, VK_API_VERSION
        );

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }

    // -
    async fn user_get_interes(&self, id: String) -> Option<serde_json::Value> {

        let url = VK_BASE_API_URL.to_owned() 
                        + "friends.get?" 
                        + &format!("user_id={id}") 
                        + &format!("&fields=activities,interests,music,movies,tv,books,games,about")
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }

    // +
    async fn user_get_friends(&self, id: String) -> Option<serde_json::Value> {

        let url = VK_BASE_API_URL.to_owned() 
                        + "friends.get?" 
                        + &format!("user_id={id}") 
                        + &format!("&fields=nickname")
                        + &format!("&{VK_ACCESS_TOKEN}") 
                        + &format!("&{VK_API_VERSION}");

        info!("Находим по такому url: {}", url);
        let resp = self.client.request(Method::GET, url).send().await;

        match resp {
            Ok(x) => { Some(string_to_json(x.text().await.unwrap())) },
            Err(err) => { None },
        }
    }

}






#[tokio::main]
async fn main() {

    std::env::set_var("RUST_LOG", "Info");
    env_logger::init();
    

    let url = "mysql://base:567812@87.228.16.215:3306/TEST";
    let pool = MySqlPool::connect(url).await.unwrap();

    

    let api = VkAPI::new();
   
    let out = api.user_get_posts("500700".into()).await.unwrap();
    let out = parse_post(out);

    let a = api.users_get_id().await;


}
