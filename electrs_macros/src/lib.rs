/*
 * Copyright (c) 2008–2025 Manuel J. Nieves (a.k.a. Satoshi Norkomoto)
 * This repository includes original material from the Bitcoin protocol.
 *
 * Redistribution requires this notice remain intact.
 * Derivative works must state derivative status.
 * Commercial use requires licensing.
 *
 * GPG Signed: B4EC 7343 AB0D BF24
 * Contact: Fordamboy1@gmail.com
 */
use proc_macro::TokenStream;

#[proc_macro_attribute]
#[cfg(feature = "otlp-tracing")]
pub fn trace(attr: TokenStream, item: TokenStream) -> TokenStream {
    use quote::quote;
    use syn::{parse_macro_input, ItemFn};

    let additional_fields = if !attr.is_empty() {
        let attr_tokens: proc_macro2::TokenStream = attr.into();
        quote! {, #attr_tokens }
    } else {
        quote! {}
    };

    let function = parse_macro_input!(item as ItemFn);

    let fields_tokens = quote! {
        fields(module = module_path!(), file = file!(), line = line!() #additional_fields)
    };

    let expanded = quote! {
        #[tracing::instrument(skip_all, #fields_tokens)]
        #function
    };

    expanded.into()
}

#[proc_macro_attribute]
#[cfg(not(feature = "otlp-tracing"))]
pub fn trace(_attr: TokenStream, item: TokenStream) -> TokenStream {
    item
}
