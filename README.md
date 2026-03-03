# regiondb

`regiondb` is a Go project for storing chunk and grid data for games and
grid-based simulations.

The project is currently a repository scaffold. The `regiondb` executable
prints its development version and exits:

```text
regiondb dev
```

The packed chunk core and `fs_split_v1` storage are available as internal Go
APIs. Networking and protocol behavior are not implemented yet.

## Development

The project requires Go 1.24 or later.

```sh
go test ./...
go build ./...
```

## License

regiondb is available under the MIT License.
