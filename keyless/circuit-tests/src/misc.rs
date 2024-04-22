
// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::TestCircuitHandle;
use aptos_keyless_common::input_processing::{
    circuit_input_signals::CircuitInputSignals, config::CircuitPaddingConfig,
};
use ark_bn254::Fr;
use ark_ff::{BigInteger, PrimeField};
use rand_chacha::{
    rand_core::{RngCore as _, SeedableRng as _},
    ChaCha20Rng,
};

const TEST_RNG_SEED: u64 = 6896401633680249901;

#[test]
fn is_whitespace_test() {
    let circuit_handle = TestCircuitHandle::new("misc/is_whitespace_test.circom").unwrap();
    let bits_max_size = 8;


    for c in 0u8..=127u8 {
        let config = CircuitPaddingConfig::new().max_length("bits_out", bits_max_size);

        let circuit_input_signals = CircuitInputSignals::new()
            .byte_input("char", c)
            .bool_input("result", (c as char).is_whitespace())
            .pad(&config)
            .unwrap();

        let result = circuit_handle.gen_witness(circuit_input_signals);
        println!("{}: {:?}", c, result);
        assert!(result.is_ok());
    }
}
