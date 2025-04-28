mod pingpong;
mod chat;

use std::{error::Error, time::Duration};
use futures::prelude::*;
use crate::chat::chat;
use crate::pingpong::ping_pong;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
  //ping_pong().await
  chat().await
}
