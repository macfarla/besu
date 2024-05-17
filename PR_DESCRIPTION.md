Implemented stricter JSON parsing for the genesis file to address issue #5900. Each account within the 'alloc' section of the genesis file now requires a 'balance' field. Unit tests have been added to ensure robustness of the new validation logic.

Link to Devin run: https://preview.devin.ai/devin/e091dae59c8d4c0b95f7f3c892335a14
