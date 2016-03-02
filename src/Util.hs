{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Util where

import Conduit
import Control.Monad
import Control.Monad.Primitive

import qualified Data.Vector.Generic as V

conduitVectorBounded :: ( MonadBase base m, V.Vector v a, PrimMonad base
                        , Num size, Ord size)
                     => (a -> size) -- ^ function to get element size
                     -> size -- ^ maximum allowed vector size
                     -> Int -- ^ maximum allowed length
                     -> Conduit a m (v a)
conduitVectorBounded f size len = loop where
  loop = do
    v <- sinkVectorN len
    unless (V.null v) $ do
      yieldMany $ splitBySize f size v
      loop
{-# INLINE conduitVectorBounded #-}

splitBySize :: (V.Vector v a, Num size, Ord size)
            => (a -> size) -> size -> v a -> [v a]
splitBySize getSize maxSize v
  | V.null v  = []
  | V.null v1 = splitV (V.drop 1 v)
  | otherwise = v1 : splitV v2
  where
    (v1, v2) = V.splitAt (maxSizeIndex getSize maxSize v) v
    splitV = splitBySize getSize maxSize
{-# INLINE splitBySize #-}

maxSizeIndex :: forall v a size. (V.Vector v a, Num size, Ord size)
             => (a -> size) -> size -> v a -> Int
maxSizeIndex getSize maxSize = snd . V.ifoldl' f (0,0) where
  f :: (size, Int) -> Int -> a -> (size, Int)
  f acc i e = let s = fst acc + getSize e
              in (s,) (if s > maxSize then snd acc else i)
{-# INLINE maxSizeIndex #-}
