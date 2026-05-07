import json

from dashboard.shm_direct_price_reader import load_symbol_map


def test_load_symbol_map_includes_contract_multiplier(tmp_path) -> None:
    refdata_path = tmp_path / "refdata.json"
    refdata_path.write_text(
        json.dumps(
            [
                {
                    "global_instance_id": 101,
                    "flat_id": "BTCUSDT-PERP",
                    "contract_multiplier": 0.001,
                },
                {
                    "symbol_id": 202,
                    "native_id": "ETHUSDT",
                    "contract_size": "10",
                },
                {
                    "symbol_id": 303,
                    "name": "XRPUSDT",
                },
            ]
        )
    )

    symbol_map = load_symbol_map(str(refdata_path))

    assert symbol_map[101] == ("BTCUSDT-PERP", 0.001)
    assert symbol_map[202] == ("ETHUSDT", 10.0)
    assert symbol_map[303] == ("XRPUSDT", 1.0)
