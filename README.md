# Iroh Share

Inspired by AirDrop I built Iroh Share in Rust.

It enables vendor-independent file sharing with receiver approval. Devices are
discovered via mDNS or connected using an
[Iroh endpoint ticket](https://github.com/n0-computer/iroh-tickets).

[Iroh](https://www.iroh.computer/) offers encrypted peer-to-peer connections with
automatic [NAT traversal](https://docs.iroh.computer/concepts/nat-traversal) and
relay fallback, while [egui](https://www.egui.rs/) powers the desktop UI.

## Screenshots

| Nearby devices                                 | Sending a file                                  |
| ---------------------------------------------- | ----------------------------------------------- |
| ![Nearby devices](docs/screenshots/nearby.png) | ![Sending a file](docs/screenshots/sending.png) |

## Roadmap

- [x] Nearby device discovery
- [x] Peer-to-peer file transfer
  - [x] Custom protocol for transfer offers and receiver decisions
  - [x] CLI commands
  - [ ] Multiple files per transfer
- [x] Desktop UI
  - [x] Drag and drop
  - [x] Polish the UI
  - [x] Transfer progress
- [x] Receiver approval
- [x] Custom download location
- [x] Connections beyond the local network
  - [x] Transfer via endpoint ticket
  - [ ] Transfer via numeric code
- [ ] Automated tests
- [ ] Packaged desktop releases
- [ ] Mobile support

## Run

Install [Rust](https://www.rust-lang.org/tools/install).

### UI

Start the desktop app on both devices:

```bash
cargo run
```

Select a nearby device or use **Copy Ticket** and **Use Ticket**, then choose or
drop a file. The receiver can accept or decline the transfer and select where
to save it.

### CLI

```bash
cargo run -- send <FILE> [TICKET]
cargo run -- receive [DOWNLOAD_DIR]
cargo run -- --help
cargo run -- --version
```

The receiver prints its ticket on startup. Unanswered transfer offers time out
after 60 seconds.

## Platforms

- macOS — tested
- Linux — tested
- Windows — expected to work, not yet tested

## Architecture

```text
mDNS discovery
      │
      ▼
Sender ── offer ──▶ Receiver
Sender ◀─ decision ─ Receiver
Sender ── blob ───▶ Receiver
```

| Module        | Responsibility                         |
| ------------- | -------------------------------------- |
| `mdns.rs`     | Discovers nearby devices               |
| `protocol.rs` | Encodes offers and transfer responses  |
| `sender.rs`   | Imports and sends files                |
| `receiver.rs` | Approves, downloads, and exports files |
| `ui.rs`       | Desktop interface                      |
| `cli.rs`      | Terminal interface                     |

## References

The initial implementation was informed by Iroh's documentation and examples:

- [Write your own protocol](https://docs.iroh.computer/protocols/writing-a-protocol)
- [mDNS address lookup](https://docs.rs/iroh-mdns-address-lookup/latest/iroh_mdns_address_lookup/)
- [Connect two endpoints](https://docs.iroh.computer/connect-two-endpoints)

Minimal experiments based on these resources are kept in [`examples`](examples).

## License

Licensed under the [MIT License](LICENSE).
