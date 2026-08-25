use clap::Parser;
use distributed_kv::server::{self, ServerConfig};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    // Port the node will use
    #[arg(short, long)]
    port: usize,

    #[arg(short, long, num_args = 1.., value_delimiter = ',')]
    nodes: Vec<String>,

    #[arg(short, long)]
    id: String,

    // Port for the client-facing HTTP API. Defaults to `port + 1000`.
    #[arg(long)]
    http_port: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let http_port = args.http_port.unwrap_or(args.port + 1000);

    server::run(ServerConfig {
        id: args.id,
        peers: args.nodes,
        grpc_addr: format!("127.0.0.1:{}", args.port).parse()?,
        http_addr: format!("127.0.0.1:{http_port}").parse()?,
    })
    .await
}
