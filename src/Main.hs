{-# OPTIONS_GHC -fno-warn-orphans  #-}

{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import BasicPrelude          hiding (empty)
import Conduit
import Control.Applicative
import Data.Aeson
import Data.Default.Generics
import Data.Time
import Database.Bloodhound   hiding (source)
import Network.HTTP.Client
import Network.HTTP.Types
import Turtle.Options

import qualified Data.ByteString.Lazy as B
import qualified Data.Text.Lazy as LT
import qualified Data.Vector as V

data Options = Options
  { sourceServer       :: Maybe Server
  , sourceIndex        :: IndexName
  , sourceMapping      :: MappingName
  , destinationIndex   :: IndexName
  , destinationMapping :: Maybe MappingName
  , destinationServer  :: Maybe Server
  , frameLength        :: Maybe Size
  , bulkSize           :: Maybe Int64
  , scrollTime         :: Maybe NominalDiffTime
  , scrollOldId        :: Maybe ScrollId
  , filter_            :: Maybe Input
  } deriving (Show, Eq)

parser :: Parser Options
parser = Options
  <$> optional (Server <$> optText "source-server" 's' empty)
  <*> (IndexName <$> argText "source-index" empty)
  <*> (MappingName <$> argText "source-mapping" empty)
  <*> (IndexName <$> argText "destination-index" empty)
  <*> optional (MappingName <$> argText "destination-mapping" empty)
  <*> optional (Server <$> optText "destination-server" 'd' empty)
  <*> optional (Size <$> optInt "framelength" 'l' empty)
  <*> optional (fromIntegral <$> optInt "bulksize" 'b'
                "Maximum total byte size of documents per one indexing request")
  <*> optional (fromIntegral <$> optInt "scrollttl" 't' "Time to keep scroll open between scans")
  <*> optional (ScrollId <$> optText "scrollid" 'i' "Open scroll id to start indexing from")
  <*> optional ((\x -> if x == "-" then Stdin else File x)
                  <$> optText "filter" 'f' "Specify filepath or - for STDIN")

main :: IO ()
main = do
  opts <- options "Re-index Elasticsearch documents using scan-and-scroll API" parser
  manager <- newManager defaultManagerSettings
  filtStr <- case filter_ opts of
               Nothing -> return Nothing
               Just Stdin -> Just . LT.toStrict <$> getContents
               Just (File p) -> Just <$> readFile (textToString p)
  let mkBH s = mkBHEnv (fromMaybe (Server "http://127.0.0.1:9200") s) manager
      sourceBH = mkBH $ sourceServer opts
      destBH = mkBH $ destinationServer opts
      filt = read <$> filtStr
      searchLength@(Size lengthN) = fromMaybe (Size 100) (frameLength opts)
      search = def { size = searchLength, filterBody = filt }
      bulkSize' = fromMaybe 78643200 $ bulkSize opts
      getScroll = runBH sourceBH $
        getInitialScroll (sourceIndex opts) (sourceMapping opts) search
  msId <- maybe getScroll (return . Just) (scrollOldId opts)
  case msId of
    Nothing -> putStrLn "No documents to scan"
    Just sId@(ScrollId rawId) -> do
      putStrLn $ "Scroll ID is " <> rawId
      runConduit $
        advanceScrollSource sourceBH sId (fromMaybe 60 $ scrollTime opts)
        =$ conduitVector lengthN
        =$ concatMapC (snd . splitByAccumResult docSize bulkSize')
        =$ mapC (V.map $ \(i,s) -> BulkIndex
                                   (destinationIndex opts)
                                   (fromMaybe (sourceMapping opts) (destinationMapping opts))
                                   i s)
        =$ mapMC (bulk' destBH)
        $$ sinkNull


docSize :: ToJSON a => (t, a) -> Int64
docSize (_, v) = B.length $ encode v

bulk' :: (MonadIO m) => BHEnv -> Vector BulkOperation -> m ()
bulk' env v = do
  putStr $ "Indexing " <> show (length v) <> " documents.. "
  r <- runBH env $ bulk v
  let s = responseStatus r
      msg = show (statusCode s) <> " " <> decodeUtf8 (statusMessage s)
  putStrLn msg

advanceScrollSource :: (MonadIO m, MonadThrow m)
                    => BHEnv
                    -> ScrollId
                    -> NominalDiffTime
                    -> Conduit.Source m (DocId, Value)
advanceScrollSource env sId t = do
  r <- runBH env $ advanceScroll sId t
  case r of
    Left er -> liftIO $ print er
    Right srch -> do
      let searchDocs = getSearchDocs srch
      yieldMany searchDocs
      case (length searchDocs, scrollId srch) of
        (0, _)         -> return ()
        (_, Nothing)   -> return ()
        (_, Just sId') -> advanceScrollSource env sId' t

getSearchDocs :: SearchResult a -> [(DocId, a)]
getSearchDocs = mapMaybe (\h -> (,) <$> pure (hitDocId h) <*> hitSource h) . hits . searchHits

data Input = File Text
           | Stdin
  deriving (Show, Eq)

instance Default Search
instance Default From where
  def = From 0
instance Default Size where
  def = Size 10
instance Default SearchType where
  def = SearchTypeQueryThenFetch

-- | Split a vector into size-constrained sub-vectors. Separate
-- elements that do not fit under limit.
splitByAccumResult :: (Num r, Ord r)
                   => (a -> r)
                   -- ^ Calculate element size.
                   -> r
                   -- ^ Maximum total size of elements in a chunk.
                   -> Vector a
                   -> ([a], [Vector a])
splitByAccumResult f maxSize v =
  go 0 0 [] [] v
  where
    go i acc fatties chunks source
      | V.null source = (fatties, chunks)
      | i == V.length source = (fatties, source:chunks)
      | otherwise =
          if newAcc >= maxSize
          then
            if i == 0
            then go 0 0 ((V.head source):fatties) chunks $ V.tail source
            else go 0 0 fatties ((V.slice 0 i source):chunks) $ V.drop i source
          else go (i + 1) newAcc fatties chunks source
          where
            newAcc = acc + (f $ source V.! i)
