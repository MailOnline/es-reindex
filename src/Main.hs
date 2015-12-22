{-# OPTIONS_GHC -fno-warn-orphans  #-}

{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import BasicPrelude          hiding (empty)
import Conduit
import Control.Applicative
import Control.Monad.Reader
import Data.Aeson
import Data.Default.Generics
import Data.Time
import Database.Bloodhound
import Network.HTTP.Client
import Turtle.Options

data Options = Options
  { server             :: Maybe Server
  , sourceIndex        :: IndexName
  , sourceMapping      :: MappingName
  , destinationIndex   :: IndexName
  , destinationMapping :: Maybe MappingName
  , frameSize          :: Maybe Size
  , scrollTime         :: Maybe NominalDiffTime
  , filter_            :: Maybe Input
  } deriving (Show, Eq)

parser :: Parser Options
parser = Options
  <$> optional (Server <$> optText "server" 's' empty)
  <*> (IndexName <$> argText "source-index" empty)
  <*> (MappingName <$> argText "source-mapping" empty)
  <*> (IndexName <$> argText "destination-index" empty)
  <*> optional (MappingName <$> argText "destination-mapping" empty)
  <*> optional (Size <$> optInt "framesize" 'n' empty)
  <*> optional (fromIntegral <$> optInt "scroll" 't' "Time to keep scroll open between scans")
  <*> optional ((\x -> if x == "-" then Stdin else File x)
                  <$> optText "filter" 'f' "Specify filepath or - for STDIN")

main :: IO ()
main = do
  opts <- options "Re-index Elasticsearch documents using scan-and-scroll API" parser
  manager <- newManager defaultManagerSettings
  filtStr <- case filter_ opts of
               Nothing -> return Nothing
               Just Stdin -> Just . fromString . ltextToString <$> getContents
               Just (File p) -> Just . encodeUtf8 <$> readFile (textToString p)
  let bhenv = BHEnv (fromMaybe (Server "http://127.0.0.1:9200") (server opts)) manager
      filt = case decodeStrict' <$> filtStr of
               Just Nothing -> error "Failed to parse filter JSON"
               x -> join x
      search = def { size = fromMaybe (Size 100) (frameSize opts), filterBody = filt }
  msId <- runBH bhenv $ getInitialScroll (sourceIndex opts) (sourceMapping opts) search
  case msId of
    Nothing -> putStrLn "No documents to scan"
    Just sId ->
      runConduit $ runReaderC bhenv $
          advanceScrollSource sId (fromMaybe 60 $ scrollTime opts)
       =$ mapC (\(i,s) -> BulkIndex (destinationIndex opts)
                                    (fromMaybe (sourceMapping opts) (destinationMapping opts))
                                    i s)
       =$ conduitVector 1000
       =$ mapMC (runBH' . bulk)
       =$ mapC responseStatus
       $$ printC

advanceScrollSource :: (MonadIO m, MonadThrow m, MonadReader BHEnv m)
                    => ScrollId -> NominalDiffTime -> Conduit.Source m (DocId, Value)
advanceScrollSource sId t = do
  r <- runBH' $ advanceScroll sId t
  case r of
    Left er -> liftIO $ print er
    Right srch -> do
      yieldMany $ getSearchDocs srch
      case scrollId srch of
        Nothing -> return ()
        Just sId' -> advanceScrollSource sId' t

getSearchDocs :: SearchResult a -> [(DocId, a)]
getSearchDocs = mapMaybe (\h -> (,) <$> pure (hitDocId h) <*> hitSource h) . hits . searchHits

runBH' :: MonadReader BHEnv m => BH m a -> m a
runBH' f = do
  e <- ask
  runBH e f

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
