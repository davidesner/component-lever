import base64

def decode_base64(encoded_str: str) -> str:
    # Decode the Base64 encoded string
    decoded_bytes = base64.b64decode(encoded_str)
    # Convert bytes to string
    decoded_str = decoded_bytes.decode('utf-8')
    return decoded_str

# Example usage
encoded_str = "TENyMTNDeDNjTGVIK200UXQzVVVqbTRxWE5PQ3RKYk9QZlVhRUdQT3RmQzFYN0RIOg=="  # "Hello World!" in Base64
decoded_str = decode_base64(encoded_str)
print(decoded_str)  # Output: Hello World!