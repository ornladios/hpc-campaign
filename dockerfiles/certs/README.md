Place any site or proxy CA certificates required for GitHub HTTPS access in
this directory before building. Use PEM format, a `.crt` extension, and one
certificate per file. Obtain the certificates from your site's trusted IT
distribution. Include any required root and intermediate CA certificates.

The integration image installs these certificates before cloning the AWS SDK
and its submodules. Leave this directory empty apart from this README on
networks that do not require a custom CA. TLS verification remains enabled.

Local `.crt` files here are ignored by Git but included in the Docker context.
