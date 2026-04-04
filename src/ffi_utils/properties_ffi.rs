// ffi_utils/properties_ffi.rs
//
// This file is part of the Eclipse Paho MQTT Rust Client library.
//
// FFI representation of MQTT v5 properties for the Paho C library.
//

/*******************************************************************************
 * Copyright (c) 2019-2026 Frank Pagliughi <fpagliughi@mindspring.com>
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v20.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Frank Pagliughi - initial implementation and documentation
 *******************************************************************************/

//! FFI representation of MQTT v5 properties for the Paho C library.

use std::{
    any::{Any, TypeId},
    ffi::CString,
    mem,
    os::raw::{c_char, c_int},
    ptr,
};

use crate::{
    ffi,
    properties::{Binary, Properties, Property, PropertyCode, PropertyType},
    Error, Result,
};

#[cfg(feature = "serde")]
use serde::{Serialize, Serializer};

/// Error code for property mismatches
const INVALID_PROPERTY_ID: i32 = ffi::MQTT_INVALID_PROPERTY_ID;

/// The Property `value` union type.
type Value = ffi::MQTTProperty__bindgen_ty_1;

/// The struct to encapsulate property string values.
type LenString = ffi::MQTTLenString;

/// The underlying data type for a specific property (C type)
type Type = ffi::MQTTPropertyTypes;

/// Local alias for the C property code integer type
type Code = ffi::MQTTPropertyCodes;

/////////////////////////////////////////////////////////////////////////////
// PropertyFfi

/// A single MQTT v5 property in FFI form.
///
/// An MQTT v5 property consists of both a property "code" and a value. The
/// code indicates what the property contains (Response Topic, Will Delay
/// Interval, etc), and also the data type for the value. Each code
/// corresponds to a single, specific data type as described in the v5
/// spec, here:
/// <https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901027>
///
/// There are only a limited number of data types that are possible for
/// properties:
///   "Byte"  - `u8`
///   "Two Byte Integer"  - `u16`
///   "Four Byte Integer"  - `u32`
///   "Binary Data"  - `Vec<u8>`
///   "UTF-8 Encoded String"  - `String`
///   "UTF-8 String Pair"  - `(String,String)`
///
#[derive(Debug)]
pub struct PropertyFfi {
    pub(crate) cprop: ffi::MQTTProperty,
}

impl PropertyFfi {
    /// Creates a new property for a given code and value.
    ///
    /// The type for the value must match the type expected for the given
    /// property code exactly, otherwise it will be rejected and return an error.
    pub fn new<T>(code: PropertyCode, val: T) -> Result<Self>
    where
        T: Any + 'static,
    {
        let rval: &(dyn Any + 'static) = &val;

        if code.type_of() == TypeId::of::<Binary>() {
            if let Some(v) = rval.downcast_ref::<&str>() {
                return Self::new_binary(code, v.as_bytes());
            }
            else if let Some(v) = rval.downcast_ref::<String>() {
                return Self::new_binary(code, v.as_bytes());
            }
        }

        if let Some(v) = rval.downcast_ref::<u8>() {
            Self::new_byte(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<u16>() {
            Self::new_u16(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<i16>() {
            Self::new_u16(code, *v as u16)
        }
        else if let Some(v) = rval.downcast_ref::<u32>() {
            Self::new_u32(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<i32>() {
            Self::new_int(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<Binary>() {
            Self::new_binary(code, v.clone())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8]>() {
            Self::new_binary(code, *v)
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 1]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 2]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 3]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 4]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 5]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 6]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 7]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 8]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 9]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 10]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 11]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 12]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 13]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 14]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 15]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 16]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 17]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 18]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 19]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 20]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 21]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 22]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 23]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 24]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 25]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 26]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 27]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 28]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 29]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 30]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 31]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<&[u8; 32]>() {
            Self::new_binary(code, v.to_vec())
        }
        else if let Some(v) = rval.downcast_ref::<String>() {
            Self::new_string(code, v)
        }
        else if let Some(v) = rval.downcast_ref::<&str>() {
            Self::new_string(code, v)
        }
        else if let Some(v) = rval.downcast_ref::<(String, String)>() {
            Self::new_string_pair(code, &v.0, &v.1)
        }
        else if let Some(v) = rval.downcast_ref::<(&str, &str)>() {
            Self::new_string_pair(code, v.0, v.1)
        }
        else if let Some(v) = rval.downcast_ref::<(&str, String)>() {
            Self::new_string_pair(code, v.0, &v.1)
        }
        else if let Some(v) = rval.downcast_ref::<(String, &str)>() {
            Self::new_string_pair(code, &v.0, v.1)
        }
        else {
            Err(INVALID_PROPERTY_ID.into())
        }
    }

    /// Creates a single-byte property
    pub fn new_byte(code: PropertyCode, val: u8) -> Result<Self> {
        match code.property_type() {
            PropertyType::Byte => Self::new_int(code, val as i32),
            _ => Err(INVALID_PROPERTY_ID.into()),
        }
    }

    /// Creates a 2-byte integer property
    pub fn new_u16(code: PropertyCode, val: u16) -> Result<Self> {
        match code.property_type() {
            PropertyType::TwoByteInteger => Self::new_int(code, val as i32),
            _ => Err(INVALID_PROPERTY_ID.into()),
        }
    }

    /// Creates a 4-byte integer property
    pub fn new_u32(code: PropertyCode, val: u32) -> Result<Self> {
        match code.property_type() {
            PropertyType::FourByteInteger => Self::new_int(code, val as i32),
            _ => Err(INVALID_PROPERTY_ID.into()),
        }
    }

    /// Creates a new integer property.
    ///
    /// This works for any sized integer type, from byte on up.
    pub fn new_int(code: PropertyCode, val: i32) -> Result<Self> {
        let value = match code.property_type() {
            PropertyType::Byte => {
                if val & !0xFF != 0 {
                    bail!(INVALID_PROPERTY_ID);
                }
                Value { byte: val as u8 }
            }
            PropertyType::TwoByteInteger => {
                if val & !0xFFFF != 0 {
                    return Err(INVALID_PROPERTY_ID.into());
                }
                Value {
                    integer2: val as u16,
                }
            }
            PropertyType::FourByteInteger | PropertyType::VariableByteInteger => Value {
                integer4: val as u32,
            },
            _ => return Err(INVALID_PROPERTY_ID.into()),
        };

        Ok(Self {
            cprop: ffi::MQTTProperty {
                identifier: code as Code,
                value,
            },
        })
    }

    /// Creates a new binary property.
    pub fn new_binary<V>(code: PropertyCode, bin: V) -> Result<Self>
    where
        V: Into<Binary>,
    {
        if code.property_type() != PropertyType::BinaryData {
            return Err(INVALID_PROPERTY_ID.into());
        }

        let mut v = bin.into();
        v.shrink_to_fit();

        let n = v.len();
        let p = v.as_mut_ptr() as *mut c_char;
        mem::forget(v);

        Ok(Self::new_string_binary(code, p, n, ptr::null_mut(), 0))
    }

    /// Creates a new string property.
    pub fn new_string(code: PropertyCode, s: &str) -> Result<Self> {
        if code.property_type() != PropertyType::Utf8EncodedString {
            return Err(INVALID_PROPERTY_ID.into());
        }

        let n = s.len();
        let p = CString::new(s).unwrap().into_raw();

        Ok(Self::new_string_binary(code, p, n, ptr::null_mut(), 0))
    }

    /// Creates a new string pair property.
    pub fn new_string_pair(code: PropertyCode, key: &str, val: &str) -> Result<Self> {
        if code.property_type() != PropertyType::Utf8StringPair {
            return Err(INVALID_PROPERTY_ID.into());
        }

        let nkey = key.len();
        let pkey = CString::new(key).unwrap().into_raw();

        let nval = val.len();
        let pval = CString::new(val).unwrap().into_raw();

        Ok(Self::new_string_binary(code, pkey, nkey, pval, nval))
    }

    /// Creates a property from a C lib MQTTProperty struct.
    pub(crate) fn from_c_property(cprop: &ffi::MQTTProperty) -> Result<Self> {
        use PropertyType::*;

        let mut cprop = *cprop;
        let typ = match PropertyCode::new(cprop.identifier).map(|c| c.property_type()) {
            Some(typ) => typ,
            None => return Err(INVALID_PROPERTY_ID.into()),
        };

        unsafe {
            let mut pdata = cprop.value.__bindgen_anon_1.data.data;
            let n = cprop.value.__bindgen_anon_1.data.len as usize;

            match typ {
                BinaryData => {
                    if pdata.is_null() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    let v = Vec::from_raw_parts(pdata, n, n);
                    let mut vc = v.clone();
                    pdata = vc.as_mut_ptr() as *mut c_char;
                    mem::forget(v);
                    mem::forget(vc);
                }
                Utf8EncodedString => {
                    if pdata.is_null() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    let v = Vec::from_raw_parts(pdata as *mut u8, n, n);
                    let sr = CString::new(v.clone());
                    if sr.is_err() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    pdata = sr.unwrap().into_raw();
                    mem::forget(v);
                }
                Utf8StringPair => {
                    let pvalue = cprop.value.__bindgen_anon_1.value.data;
                    if pdata.is_null() || pvalue.is_null() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }

                    let v = Vec::from_raw_parts(pdata as *mut u8, n, n);
                    let sr = CString::new(v.clone());
                    if sr.is_err() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    pdata = sr.unwrap().into_raw();
                    mem::forget(v);

                    let n = cprop.value.__bindgen_anon_1.value.len as usize;
                    let v = Vec::from_raw_parts(pvalue as *mut u8, n, n);
                    let sr = CString::new(v.clone());
                    if sr.is_err() {
                        return Err(INVALID_PROPERTY_ID.into());
                    }
                    cprop.value.__bindgen_anon_1.value.data = sr.unwrap().into_raw();
                    mem::forget(v);
                }
                _ => (),
            }

            cprop.value.__bindgen_anon_1.data.data = pdata;
        }
        Ok(Self { cprop })
    }

    /// Creates a new string, string pair, or binary property given the raw
    /// pointers and sizes.
    fn new_string_binary(
        code: PropertyCode,
        pdata: *mut c_char,
        ndata: usize,
        pval: *mut c_char,
        nval: usize,
    ) -> Self {
        Self {
            cprop: ffi::MQTTProperty {
                identifier: code as Code,
                value: Value {
                    __bindgen_anon_1: ffi::MQTTProperty__bindgen_ty_1__bindgen_ty_1 {
                        data: LenString {
                            len: ndata as c_int,
                            data: pdata,
                        },
                        value: LenString {
                            len: nval as c_int,
                            data: pval,
                        },
                    },
                },
            },
        }
    }

    /// Gets the MQTT code for the property.
    pub fn property_code(&self) -> PropertyCode {
        PropertyCode::new(self.cprop.identifier).unwrap()
    }

    /// Gets the type of this property.
    pub fn property_type(&self) -> PropertyType {
        self.property_code().property_type()
    }

    /// Gets the any::TypeId of this property.
    pub fn type_of(&self) -> TypeId {
        self.property_type().type_of()
    }

    /// Gets the property value
    pub fn get<T>(&self) -> Option<T>
    where
        T: Any + 'static + Send + Default,
    {
        let mut v = T::default();
        let x: &mut dyn Any = &mut v;

        if let Some(val) = x.downcast_mut::<u8>() {
            if let Some(n) = self.get_byte() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<u16>() {
            if let Some(n) = self.get_u16() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<u32>() {
            if let Some(n) = self.get_u32() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<i32>() {
            if let Some(n) = self.get_int() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<Binary>() {
            if let Some(n) = self.get_binary() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<String>() {
            if let Some(n) = self.get_string() {
                *val = n;
                return Some(v);
            }
        }
        else if let Some(val) = x.downcast_mut::<(String, String)>() {
            if let Some(n) = self.get_string_pair() {
                *val = n;
                return Some(v);
            }
        }
        None
    }

    /// Gets the property value as a byte.
    pub fn get_byte(&self) -> Option<u8> {
        match self.property_type() {
            PropertyType::Byte => Some(unsafe { self.cprop.value.byte }),
            _ => None,
        }
    }

    /// Gets the property value as a u16.
    pub fn get_u16(&self) -> Option<u16> {
        match self.property_type() {
            PropertyType::TwoByteInteger => Some(unsafe { self.cprop.value.integer2 }),
            _ => None,
        }
    }

    /// Gets the property value as a u32.
    pub fn get_u32(&self) -> Option<u32> {
        match self.property_type() {
            PropertyType::FourByteInteger => Some(unsafe { self.cprop.value.integer4 }),
            _ => None,
        }
    }

    /// Gets the property value as an integer.
    pub fn get_int(&self) -> Option<i32> {
        unsafe {
            match self.property_type() {
                PropertyType::Byte => Some(self.cprop.value.byte as i32),
                PropertyType::TwoByteInteger => Some(self.cprop.value.integer2 as i32),
                PropertyType::FourByteInteger | PropertyType::VariableByteInteger => {
                    Some(self.cprop.value.integer4 as i32)
                }
                _ => None,
            }
        }
    }

    /// Gets the property value as a binary blob.
    pub fn get_binary(&self) -> Option<Binary> {
        unsafe {
            if self.property_type() == PropertyType::BinaryData {
                let n = self.cprop.value.__bindgen_anon_1.data.len as usize;
                let p = self.cprop.value.__bindgen_anon_1.data.data as *mut u8;
                let v = Vec::from_raw_parts(p, n, n);
                let vc = v.clone();
                mem::forget(v);
                Some(vc)
            }
            else {
                None
            }
        }
    }

    /// Gets the property value as a string.
    pub fn get_string(&self) -> Option<String> {
        unsafe {
            if self.property_type() == PropertyType::Utf8EncodedString {
                let s = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                let sc = s.clone();
                let _ = s.into_raw();
                sc.into_string().ok()
            }
            else {
                None
            }
        }
    }

    /// Gets the property value as a string pair.
    pub fn get_string_pair(&self) -> Option<(String, String)> {
        unsafe {
            if self.property_type() == PropertyType::Utf8StringPair {
                let s = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                let sc = s.clone();
                let _ = s.into_raw();
                let keyopt = sc.into_string().ok();

                let s = CString::from_raw(self.cprop.value.__bindgen_anon_1.value.data);
                let sc = s.clone();
                let _ = s.into_raw();
                let valopt = sc.into_string().ok();
                keyopt.and_then(|key| valopt.map(|val| (key, val)))
            }
            else {
                None
            }
        }
    }
}

impl Drop for PropertyFfi {
    /// Drops the property, freeing any heap-allocated strings or binary data.
    fn drop(&mut self) {
        unsafe {
            match self.property_type() {
                PropertyType::BinaryData => {
                    trace!(
                        "Dropping binary property: {:?}",
                        self.cprop.value.__bindgen_anon_1.data.data
                    );
                    let n = self.cprop.value.__bindgen_anon_1.data.len as usize;
                    let _ = Vec::from_raw_parts(
                        self.cprop.value.__bindgen_anon_1.data.data,
                        n,
                        n,
                    );
                }
                PropertyType::Utf8EncodedString => {
                    trace!(
                        "Dropping string property: {:?}",
                        self.cprop.value.__bindgen_anon_1.data.data
                    );
                    let _ = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                }
                PropertyType::Utf8StringPair => {
                    trace!(
                        "Dropping string pair property: {:?}, {:?}",
                        self.cprop.value.__bindgen_anon_1.data.data,
                        self.cprop.value.__bindgen_anon_1.value.data
                    );
                    let _ = CString::from_raw(self.cprop.value.__bindgen_anon_1.data.data);
                    let _ = CString::from_raw(self.cprop.value.__bindgen_anon_1.value.data);
                }
                _ => (),
            }
        }
    }
}

impl Clone for PropertyFfi {
    /// Creates a clone of the property, also cloning any heap-allocated data.
    fn clone(&self) -> Self {
        let mut cprop = self.cprop;

        unsafe {
            match self.property_type() {
                PropertyType::BinaryData => {
                    let n = cprop.value.__bindgen_anon_1.data.len as usize;
                    let v =
                        Vec::from_raw_parts(cprop.value.__bindgen_anon_1.data.data, n, n);
                    let mut vc = v.clone();
                    let p = vc.as_mut_ptr() as *mut c_char;
                    cprop.value.__bindgen_anon_1.data.data = p;
                    mem::forget(v);
                    mem::forget(vc);
                }
                PropertyType::Utf8EncodedString => {
                    let s = CString::from_raw(cprop.value.__bindgen_anon_1.data.data);
                    let sc = s.clone();
                    let _ = s.into_raw();
                    cprop.value.__bindgen_anon_1.data.data = sc.into_raw();
                }
                PropertyType::Utf8StringPair => {
                    let s = CString::from_raw(cprop.value.__bindgen_anon_1.data.data);
                    let sc = s.clone();
                    cprop.value.__bindgen_anon_1.data.data = sc.into_raw();
                    let _ = s.into_raw();

                    let s = CString::from_raw(cprop.value.__bindgen_anon_1.value.data);
                    let sc = s.clone();
                    cprop.value.__bindgen_anon_1.value.data = sc.into_raw();
                    let _ = s.into_raw();
                }
                _ => (),
            }
        }
        Self { cprop }
    }
}

/////////////////////////////////////////////////////////////////////////////
// PropertiesFfi

/// A collection of MQTT v5 properties in FFI form.
#[derive(Debug, Default)]
pub struct PropertiesFfi {
    pub(crate) cprops: ffi::MQTTProperties,
}

impl PropertiesFfi {
    /// Creates a new, empty collection of properties.
    pub fn new() -> Self {
        Self::default()
    }

    /// Determines if the property list has no items in it.
    pub fn is_empty(&self) -> bool {
        self.cprops.count == 0
    }

    /// Gets the number of property items in the collection.
    pub fn len(&self) -> usize {
        self.cprops.count as usize
    }

    /// Gets the number of bytes required for the serialized list on the wire.
    pub fn byte_len(&self) -> usize {
        let p = &self.cprops as *const ffi::MQTTProperties;
        unsafe { ffi::MQTTProperties_len(p) as usize }
    }

    /// Removes all the items from the property list.
    pub fn clear(&mut self) {
        unsafe { ffi::MQTTProperties_free(&mut self.cprops) };
        self.cprops = ffi::MQTTProperties::default();
    }

    /// Adds a property to the collection.
    pub fn push(&mut self, prop: PropertyFfi) -> Result<()> {
        match unsafe { ffi::MQTTProperties_add(&mut self.cprops, &prop.cprop) } {
            0 => Ok(()),
            rc => Err(rc.into()),
        }
    }

    /// Adds a property to the collection given the property code and value.
    pub fn push_val<T>(&mut self, code: PropertyCode, val: T) -> Result<()>
    where
        T: Any + 'static,
    {
        self.push(PropertyFfi::new(code, val)?)
    }

    /// Adds a single-byte property to the collection.
    pub fn push_byte(&mut self, code: PropertyCode, val: u8) -> Result<()> {
        self.push(PropertyFfi::new_byte(code, val)?)
    }

    /// Adds a two-byte integer property to the collection.
    pub fn push_u16(&mut self, code: PropertyCode, val: u16) -> Result<()> {
        self.push(PropertyFfi::new_u16(code, val)?)
    }

    /// Adds a four-byte integer property to the collection.
    pub fn push_u32(&mut self, code: PropertyCode, val: u32) -> Result<()> {
        self.push(PropertyFfi::new_u32(code, val)?)
    }

    /// Adds an integer property to the collection.
    pub fn push_int(&mut self, code: PropertyCode, val: i32) -> Result<()> {
        self.push(PropertyFfi::new_int(code, val)?)
    }

    /// Adds a binary property to the collection.
    pub fn push_binary<V>(&mut self, code: PropertyCode, bin: V) -> Result<()>
    where
        V: Into<Binary>,
    {
        self.push(PropertyFfi::new_binary(code, bin)?)
    }

    /// Adds a string property to the collection.
    pub fn push_string(&mut self, code: PropertyCode, s: &str) -> Result<()> {
        self.push(PropertyFfi::new_string(code, s)?)
    }

    /// Adds a string pair property to the collection.
    pub fn push_string_pair(
        &mut self,
        code: PropertyCode,
        key: &str,
        val: &str,
    ) -> Result<()> {
        self.push(PropertyFfi::new_string_pair(code, key, val)?)
    }

    /// Gets a property instance.
    pub fn get(&self, code: PropertyCode) -> Option<PropertyFfi> {
        self.get_at(code, 0)
    }

    /// Gets a property instance at the specified index in the collection.
    pub fn get_idx(&self, idx: usize) -> Option<PropertyFfi> {
        let ps = &self.cprops as *const _ as *mut ffi::MQTTProperties;
        unsafe {
            if idx < (*ps).count as usize {
                let p = (*ps).array.offset(idx as isize);
                PropertyFfi::from_c_property(&*p).ok()
            }
            else {
                None
            }
        }
    }

    /// Gets a property instance when there are possibly multiple values.
    pub fn get_at(&self, code: PropertyCode, idx: usize) -> Option<PropertyFfi> {
        let ps = &self.cprops as *const _ as *mut ffi::MQTTProperties;
        unsafe {
            let p = ffi::MQTTProperties_getPropertyAt(ps, code as Code, idx as c_int);
            if !p.is_null() {
                PropertyFfi::from_c_property(&*p).ok()
            }
            else {
                None
            }
        }
    }

    /// Gets an iterator for all the properties.
    pub fn iter(&self) -> PropertyIterator<'_> {
        PropertyIterator {
            props: self,
            code: None,
            idx: 0,
        }
    }

    /// Gets an iterator for a specific property code.
    pub fn code_iter(&self, code: PropertyCode) -> PropertyIterator<'_> {
        PropertyIterator {
            props: self,
            code: Some(code),
            idx: 0,
        }
    }

    /// Gets a value from the collection when there may be more than one for the code.
    pub fn get_val_at<T>(&self, code: PropertyCode, idx: usize) -> Option<T>
    where
        T: Any + 'static + Send + Default,
    {
        self.get_at(code, idx).and_then(|prop| prop.get())
    }

    /// Gets a value from the collection.
    pub fn get_val<T>(&self, code: PropertyCode) -> Option<T>
    where
        T: Any + 'static + Send + Default,
    {
        self.get_val_at(code, 0)
    }

    /// Gets an integer value of a specific property.
    pub fn get_int(&self, code: PropertyCode) -> Option<i32> {
        self.get(code).and_then(|prop| prop.get_int())
    }

    /// Gets an integer value of a specific value when there may be more than one.
    pub fn get_int_at(&self, code: PropertyCode, idx: usize) -> Option<i32> {
        self.get_at(code, idx).and_then(|prop| prop.get_int())
    }

    /// Gets a binary value of a specific property.
    pub fn get_binary(&self, code: PropertyCode) -> Option<Binary> {
        self.get(code).and_then(|prop| prop.get_binary())
    }

    /// Gets a binary value of a specific value when there may be more than one.
    pub fn get_binary_at(&self, code: PropertyCode, idx: usize) -> Option<Binary> {
        self.get_at(code, idx).and_then(|prop| prop.get_binary())
    }

    /// Gets a string value of a specific property.
    pub fn get_string(&self, code: PropertyCode) -> Option<String> {
        self.get(code).and_then(|prop| prop.get_string())
    }

    /// Gets a string value of a specific value when there may be more than one.
    pub fn get_string_at(&self, code: PropertyCode, idx: usize) -> Option<String> {
        self.get_at(code, idx).and_then(|prop| prop.get_string())
    }

    /// Gets a string pair for a specific property.
    pub fn get_string_pair(&self, code: PropertyCode) -> Option<(String, String)> {
        self.get(code).and_then(|prop| prop.get_string_pair())
    }

    /// Gets a string pair for a specific property when there may be more than one.
    pub fn get_string_pair_at(
        &self,
        code: PropertyCode,
        idx: usize,
    ) -> Option<(String, String)> {
        self.get_at(code, idx)
            .and_then(|prop| prop.get_string_pair())
    }

    /// Gets an iterator into the user property string pairs.
    pub fn user_iter(&self) -> StringPairIterator<'_> {
        StringPairIterator {
            props: self,
            code: PropertyCode::UserProperty,
            idx: 0,
        }
    }

    /// Searches for the specified key in the user properties and returns
    /// the value if found.
    pub fn find_user_property(&self, key: &str) -> Option<String> {
        for (k, v) in self.user_iter() {
            if k == key {
                return Some(v);
            }
        }
        None
    }
}

impl Clone for PropertiesFfi {
    fn clone(&self) -> Self {
        let cprops = unsafe { ffi::MQTTProperties_copy(&self.cprops) };
        Self { cprops }
    }
}

impl Drop for PropertiesFfi {
    fn drop(&mut self) {
        unsafe { ffi::MQTTProperties_free(&mut self.cprops) };
    }
}

impl From<ffi::MQTTProperties> for PropertiesFfi {
    /// Creates a set of properties from an underlying C struct.
    fn from(cprops: ffi::MQTTProperties) -> Self {
        Self { cprops }
    }
}

impl From<&ffi::MQTTProperties> for PropertiesFfi {
    /// Creates a set of properties from an underlying C struct via deep copy.
    fn from(cprops: &ffi::MQTTProperties) -> Self {
        let cprops = unsafe { ffi::MQTTProperties_copy(cprops) };
        Self { cprops }
    }
}

/// Iterator over all properties (or a specific code) in a [`PropertiesFfi`].
pub struct PropertyIterator<'a> {
    props: &'a PropertiesFfi,
    code: Option<PropertyCode>,
    idx: usize,
}

impl<'a> Iterator for PropertyIterator<'a> {
    type Item = PropertyFfi;

    fn next(&mut self) -> Option<Self::Item> {
        let prop = match self.code {
            Some(code) => self.props.get_at(code, self.idx),
            None => self.props.get_idx(self.idx),
        };
        self.idx += 1;
        prop
    }
}

/// Iterator over user string-pair properties in a [`PropertiesFfi`].
pub struct StringPairIterator<'a> {
    props: &'a PropertiesFfi,
    code: PropertyCode,
    idx: usize,
}

impl<'a> Iterator for StringPairIterator<'a> {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        let pair = self.props.get_string_pair_at(self.code, self.idx);
        self.idx += 1;
        pair
    }
}

#[cfg(feature = "serde")]
impl Serialize for PropertiesFfi {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use PropertyType::*;

        let mut state = serializer.serialize_struct("Properties", self.len())?;
        for prop in self.iter() {
            match prop.property_type() {
                Byte | TwoByteInteger | FourByteInteger | VariableByteInteger => {
                    let name = prop.property_code().to_string();
                    state.serialize_field(&name, prop.get_int())?;
                }
                _ => {
                    state.serialize_field("xxx", 0)?;
                }
            }
        }
        state.end()
    }
}

impl From<&Properties> for PropertiesFfi {
    fn from(props: &Properties) -> Self {
        use PropertyType::*;

        let mut ffi_props = Self::new();
        for prop in props.iter() {
            let code = prop.property_code();

            match prop.property_type() {
                Byte => {
                    let val = prop.get_byte().unwrap();
                    ffi_props.push_byte(code, *val).unwrap();
                }
                TwoByteInteger => {
                    let val = prop.get_u16().unwrap();
                    ffi_props.push_u16(code, *val).unwrap();
                }
                FourByteInteger | VariableByteInteger => {
                    let val = prop.get_u32().unwrap();
                    ffi_props.push_u32(code, *val).unwrap();
                }
                BinaryData => {
                    let val = prop.get_binary().unwrap();
                    ffi_props.push_binary(code, val).unwrap();
                }
                Utf8EncodedString => {
                    let val = prop.get_string().unwrap();
                    ffi_props.push_string(code, val).unwrap();
                }
                Utf8StringPair => {
                    let (key, val) = prop.get_string_pair().unwrap();
                    ffi_props.push_string_pair(code, key, val).unwrap();
                }
            }
        }
        ffi_props
    }
}

impl From<Properties> for PropertiesFfi {
    fn from(props: Properties) -> Self {
        Self::from(&props)
    }
}

/// Converts an `ffi::MQTTProperties` C struct to the pure Rust [`Properties`] type.
impl From<&ffi::MQTTProperties> for Properties {
    fn from(cprops: &ffi::MQTTProperties) -> Self {
        use PropertyType::*;

        let mut props = Self::new();
        let ffi_props = PropertiesFfi::from(cprops);

        for prop in ffi_props.iter() {
            let code = prop.property_code();

            match prop.property_type() {
                Byte => {
                    let val = prop.get_byte().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                TwoByteInteger => {
                    let val = prop.get_u16().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                FourByteInteger | VariableByteInteger => {
                    let val = prop.get_u32().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                BinaryData => {
                    let val = prop.get_binary().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                Utf8EncodedString => {
                    let val = prop.get_string().unwrap();
                    props.push(Property::new(code, val).unwrap());
                }
                Utf8StringPair => {
                    let (key, val) = prop.get_string_pair().unwrap();
                    props.push(Property::new(code, (key, val)).unwrap());
                }
            }
        }
        props
    }
}
