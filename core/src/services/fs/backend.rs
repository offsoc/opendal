// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::format::Debug;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use log::debug;

use super::config::*;
use super::core::*;
use super::delete::FsDeleter;
use super::lister::FsLister;
use super::reader::FsReader;
use super::writer::FsWriter;
use super::writer::FsWriters;
use crate::raw::*;
use crate::*;

impl Configurator for FoyerConfig {
    type Builder = FoyerBuilder;
    fn into_builder(self) -> Self::Builder {
        FoyerBuilder { config: self }
    }
}

/// Foyer service support
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct FoyerBuilder {
    config: FoyerConfig,
    builder: foyer::HybridCacheBuilder<String, Buffer>,
}

impl Debug for FoyerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerBuilder")
            .field("config", &self.config)
            .finish()
    }
}

impl FoyerBuilder {
    /// Set root for backend.
    pub fn builder(mut self, builder: foyer::HybridCacheBuilder<String, Buffer>) -> Self {
        self.builder = builder;
        self
    }
}

impl Builder for FoyerBuilder {
    const SCHEME: Scheme = Scheme::Fs;
    type Config = FoyerConfig;

    fn build(self) -> Result<impl Access> {
        let cache = self.builder.build()?;

        Ok(FsBackend {
            core: Arc::new(FsCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Fs)
                        .set_root(&root.to_string_lossy())
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_last_modified: true,

                            read: true,

                            write: true,
                            write_can_empty: true,
                            write_can_append: true,
                            write_can_multi: true,
                            write_with_if_not_exists: true,

                            create_dir: true,
                            delete: true,

                            list: true,

                            copy: true,
                            rename: true,
                            blocking: true,

                            shared: true,

                            ..Default::default()
                        });

                    am.into()
                },
                root,
                atomic_write_dir,
                buf_pool: oio::PooledBuf::new(16).with_initial_capacity(256 * 1024),
            }),
        })
    }
}

/// Backend is used to serve `Accessor` support for posix-like fs.
#[derive(Debug, Clone)]
pub struct FsBackend {
    core: Arc<FsCore>,
}
