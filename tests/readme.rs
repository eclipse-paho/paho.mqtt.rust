// tests/readme.rs
//
// The code blocks from README.md, so that they cannot quietly stop
// compiling. Nothing here runs against a broker: reaching the end of
// compilation is the whole test.
//
// Keep in step with README.md when either changes.
#![allow(unused)]
// Every block builds a client, which needs a runtime backend.
#![cfg(any(feature = "tokio", feature = "smol"))]
use paho_mqtt as mqtt;
use std::time::Duration;

// ---- block 0: async quick start ----
mod async_start {
    use super::*;

    async fn main() -> mqtt::Result<()> {
        let mut cli = mqtt::AsyncClient::new("mqtt://localhost:1883")?;

        // Take the message stream before connecting, so nothing is missed.
        let strm = cli.get_stream(25);

        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_start(true)
            .finalize();

        cli.connect(conn_opts).await?;
        cli.subscribe("test/#", mqtt::QOS_1).await?;

        cli.publish(mqtt::Message::new(
            "test/hello",
            "Hello world!",
            mqtt::QOS_1,
        ))
        .await?;

        // `None` arrives if the connection drops; the client reconnects if
        // configured to, and the stream continues.
        while let Ok(Some(msg)) = strm.recv().await {
            println!("{}: {}", msg.topic(), msg.payload_str());
        }

        cli.disconnect(None).await?;
        Ok(())
    }
}

// ---- block 1: blocking quick start ----
mod blocking_start {
    use super::*;

    fn main() -> mqtt::Result<()> {
        let cli = mqtt::Client::new("mqtt://localhost:1883")?;

        cli.connect(None)?;
        cli.publish(mqtt::Message::new(
            "test/hello",
            "Hello world!",
            mqtt::QOS_1,
        ))?;
        cli.disconnect(None)?;
        Ok(())
    }
}

// ---- fragments, in a scope that has a client and a message ----
mod fragments {
    use super::*;
    fn outer() -> mqtt::Result<()> {
        let msg = mqtt::Message::new("t", "p", mqtt::QOS_1);
        let cli = mqtt::AsyncClient::new("mqtt://localhost:1883")?;
        let _f2 = || -> mqtt::Result<()> {
            let tok = cli.publish(msg); // already on its way
            // ... do something else ...
            tok.wait()?; // or: tok.await?;  or: tok.try_wait()
            Ok(())
        };
        let _f3 = || -> mqtt::Result<()> {
            let tls = mqtt::TlsOptionsBuilder::new()
                .trust_store("/path/to/ca.crt")
                .finalize();

            let conn_opts = mqtt::ConnectOptionsBuilder::new()
                .tls_options(tls)
                .finalize();
            Ok(())
        };
        let _f4 = || -> mqtt::Result<()> {
            let mut props = mqtt::Properties::new();
            props.push(mqtt::Property::SessionExpiryInterval(3600));

            let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
                .clean_start(false)
                .properties(props)
                .finalize();
            Ok(())
        };
        let _f5 = || -> mqtt::Result<()> {
            let cli = mqtt::CreateOptionsBuilder::new()
                .server_uri("mqtt://localhost:1883")
                .client_id("my-client")
                .persistence(mqtt::PersistenceType::File) // or FilePath, MemPersistence, None
                .create_async_client()?;
            Ok(())
        };
        Ok(())
    }
}

#[test]
fn readme_examples_compile() {}
