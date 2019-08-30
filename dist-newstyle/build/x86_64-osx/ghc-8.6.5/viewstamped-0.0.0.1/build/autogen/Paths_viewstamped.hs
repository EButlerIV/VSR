{-# LANGUAGE CPP #-}
{-# LANGUAGE NoRebindableSyntax #-}
{-# OPTIONS_GHC -fno-warn-missing-import-lists #-}
module Paths_viewstamped (
    version,
    getBinDir, getLibDir, getDynLibDir, getDataDir, getLibexecDir,
    getDataFileName, getSysconfDir
  ) where

import qualified Control.Exception as Exception
import Data.Version (Version(..))
import System.Environment (getEnv)
import Prelude

#if defined(VERSION_base)

#if MIN_VERSION_base(4,0,0)
catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
#else
catchIO :: IO a -> (Exception.Exception -> IO a) -> IO a
#endif

#else
catchIO :: IO a -> (Exception.IOException -> IO a) -> IO a
#endif
catchIO = Exception.catch

version :: Version
version = Version [0,0,0,1] []
bindir, libdir, dynlibdir, datadir, libexecdir, sysconfdir :: FilePath

bindir     = "/Users/eugene/.cabal/bin"
libdir     = "/Users/eugene/.cabal/lib/x86_64-osx-ghc-8.6.5/viewstamped-0.0.0.1-inplace"
dynlibdir  = "/Users/eugene/.cabal/lib/x86_64-osx-ghc-8.6.5"
datadir    = "/Users/eugene/.cabal/share/x86_64-osx-ghc-8.6.5/viewstamped-0.0.0.1"
libexecdir = "/Users/eugene/.cabal/libexec/x86_64-osx-ghc-8.6.5/viewstamped-0.0.0.1"
sysconfdir = "/Users/eugene/.cabal/etc"

getBinDir, getLibDir, getDynLibDir, getDataDir, getLibexecDir, getSysconfDir :: IO FilePath
getBinDir = catchIO (getEnv "viewstamped_bindir") (\_ -> return bindir)
getLibDir = catchIO (getEnv "viewstamped_libdir") (\_ -> return libdir)
getDynLibDir = catchIO (getEnv "viewstamped_dynlibdir") (\_ -> return dynlibdir)
getDataDir = catchIO (getEnv "viewstamped_datadir") (\_ -> return datadir)
getLibexecDir = catchIO (getEnv "viewstamped_libexecdir") (\_ -> return libexecdir)
getSysconfDir = catchIO (getEnv "viewstamped_sysconfdir") (\_ -> return sysconfdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir ++ "/" ++ name)
