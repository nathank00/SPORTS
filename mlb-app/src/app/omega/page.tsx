"use client"

import { useEffect, useRef, useState } from "react"
import { Calendar, Clock, Info, ArrowUp, ArrowDown } from "lucide-react"
import { useRouter } from "next/navigation"
import { TeamLogo } from "@/components/team-logo"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  RefreshCw,
  ExternalLink,
  Github,
} from "lucide-react"
import { useFetchCsv } from "@/components/csv-fetcher"

{/*}
type EnrichedGameData = {
  prediction: number | null
  outcome: number | null
  completed: boolean
  runs_total: number | "-" | null
}*/}


type GamePick = {
  game_id: string
  home_name: string
  away_name: string
  runline: number | null
  pick: string // "Over" | "Under" | "-"
  start_time: string | null
  runs_total: number | null
  result: string
  Away_SP_Name: string
  Home_SP_Name: string
  description?: string
  runs_home?: number | null
  runs_away?: number | null
  game_started?: boolean
  game_complete?: boolean
  label_over_under?: number
  model_prediction?: number
  prediction_confidence?: number | null
  [key: string]: string | number | boolean | null | undefined
}

export default function OmegaPage() {
  const getOutcomeStatus = (game: GamePick): string => {
    const { model_prediction, prediction_confidence, label_over_under, game_complete, runline, runs_total } = game
  
    console.log("🔍 Outcome Debug", {
      game_id: game.game_id,
      model_prediction,
      prediction_confidence,
      label_over_under,
      game_complete,
      runline,
      runs_total,
    })
  
    if (model_prediction === undefined || runline === null) return "TBD"
  
    const prediction = model_prediction
  
    if (game_complete === false) {
      if (label_over_under === undefined || label_over_under === null || prediction ===null) return "TBD"
      if (prediction === 1 && label_over_under === 1) return "W"
      if (prediction === 0 && label_over_under === 1) return "L"
      return "TBD"
    }
  
    if (runs_total === null) return "TBD"
  
    const actualTotal = Number(runs_total)
    if (Math.abs(actualTotal - runline) < 0.01) return "PUSH"
    if (prediction === null) return "TBD"
  
    const isCorrect =
      (prediction === 1 && actualTotal > runline) ||
      (prediction === 0 && actualTotal < runline)
  
    return isCorrect ? "W" : "L"
  }
  
  
  
  

  const router = useRouter()
  const datePickerRef = useRef<HTMLDivElement>(null)
  const [sidebarVisible, setSidebarVisible] = useState(true)

  const [selectedDate, setSelectedDate] = useState(() => {
    const pacificNow = new Date(new Date().toLocaleString("en-US", { timeZone: "America/Los_Angeles" }))
    const year = pacificNow.getFullYear()
    const month = String(pacificNow.getMonth() + 1).padStart(2, "0")
    const day = String(pacificNow.getDate()).padStart(2, "0")    
    return `${year}-${month}-${day}`
  })

  //const [enrichedGames, setEnrichedGames] = useState<Record<string | number, EnrichedGameData>>({})
  const [games, setGames] = useState<GamePick[]>([])
  const [lastUpdated, setLastUpdated] = useState<string>("Loading...")
  const [dailyRecord, setDailyRecord] = useState<{ wins: number; losses: number; percent: string }>({ wins: 0, losses: 0, percent: "0.0%" })
  const [allTimeRecord, setAllTimeRecord] = useState<{ wins: number; losses: number; percent: string }>({ wins: 0, losses: 0, percent: "0.0%" })
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")
  const [showDatePicker, setShowDatePicker] = useState(false)
  const [year, month, day] = selectedDate.split("-").map(Number)
  const pacificMidnight = new Date(Date.UTC(year, month - 1, day, 7, 0, 0)) // 7am UTC = midnight PT
  const formattedSelectedDate = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/Los_Angeles",
    weekday: "long",
    year: "numeric",
    month: "long",
    day: "numeric",
  }).format(pacificMidnight)



  // Fetch the last_updated.csv file
    const { data: lastUpdatedData, error: lastUpdatedError } = useFetchCsv("/last_updated.csv")
  

    useEffect(() => {
      if (lastUpdatedError) {
        console.error("Failed to fetch last updated data:", lastUpdatedError)
        setLastUpdated("Unavailable")
      } else if (lastUpdatedData && lastUpdatedData.length > 0 && lastUpdatedData[0].last_updated) {
        setLastUpdated(lastUpdatedData[0].last_updated)
      }
    }, [lastUpdatedData, lastUpdatedError])
  

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (datePickerRef.current && !datePickerRef.current.contains(event.target as Node)) {
        setShowDatePicker(false)
      }
    }
    document.addEventListener("mousedown", handleClickOutside)
    return () => document.removeEventListener("mousedown", handleClickOutside)
  }, [])

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true)
      setError("")
      try {
        const response = await fetch(`/api/omega-games?date=${selectedDate}`)
        const data = await response.json()
        if (data.error) {
          setError(data.error)
          setGames([])
          setDailyRecord({ wins: 0, losses: 0, percent: "0.0%" })
          setAllTimeRecord({ wins: 0, losses: 0, percent: "0.0%" })
        } else {
          setGames(data.gamePicks || [])
          setDailyRecord(data.dailyRecord || { wins: 0, losses: 0, percent: "0.0%" })
          setAllTimeRecord(data.allTimeRecord || { wins: 0, losses: 0, percent: "0.0%" })
        }
      } catch (err) {
        console.error("Fetch failed", err)
        setError("Failed to load data")
      } finally {
        setIsLoading(false)
      }
    }

    fetchData()
  }, [selectedDate])

  const formatTime = (time: string | null): string => {
    if (!time) return "TBD"
    try {
      const date = new Date(time)
      return date.toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
        hour12: true,
        timeZone: "America/Los_Angeles",
      })
    } catch {
      return "TBD"
    }
  }

  const renderPrediction = (pick: string | null) => {
    if (pick === "Over") {
      return <ArrowUp className="h-6 w-6 text-purple-300" />
    } else if (pick === "Under") {
      return <ArrowDown className="h-6 w-6 text-purple-300" />
    } else {
      return <span className="text-purple-300 text-base">Pending</span>
    }
  }

  return (
    <main className="omega-page flex min-h-screen bg-[#0e0414] text-purple-50 font-light">
      {sidebarVisible && (
        <aside className="w-[26rem] bg-[#080110] border-r border-gray-700/30 p-4 flex flex-col fixed h-full transition-all duration-300 ease-in-out left-0">
          <div className="mt-8 mb-8 flex justify-center w-full">
            <h1 
              className="text-2xl font-mono text-white mb-2 font-light tracking-wide cursor-pointer hover:text-purple-300 transition-colors whitespace-nowrap text-center"
              onClick={() => setSidebarVisible(false)}
            >
                [OMEGA]
            </h1>
          </div>
          <div className="mb-2 flex justify-center">
            <img src="/team-logos/monkeyking4.png" alt="Monkey King" className="w-24 h-auto" />
          </div>
          <div className="mb-12 text-center">
            <span className="text-green-400">{allTimeRecord.wins} W</span>
            <span className="text-gray-500 mx-2">|</span>
            <span className="text-red-400">{allTimeRecord.losses} L</span>
            <div className="text-sm text-purple-400 mt-1">{allTimeRecord.percent} ({allTimeRecord.wins + allTimeRecord.losses} games)</div>
          </div>

          {/* Unified Sidebar Section with Individual Boxes */}
          <div className="space-y-2">
            {/* Last Updated */}
            <div className="bg-purple-900/20 border border-purple-800/30 rounded-lg p-3 flex items-center gap-2 text-purple-300/80 font-medium">
              <RefreshCw className="h-4 w-4" />
              <span>{lastUpdated}</span>
            </div>

            {/* External Links */}
            <a
              href="https://www.cbssports.com/mlb/scoreboard/"
              target="_blank"
              rel="noopener noreferrer"
              className="bg-purple-900/20 border border-purple-800/30 rounded-lg p-3 flex items-center gap-2 text-purple-300/80 hover:text-purple-200 transition-colors font-medium"
            >
              <ExternalLink className="h-4 w-4" />
              <span>View MLB Games</span>
            </a>
            <a
              href="https://github.com/nathank00/MLB-Analytics"
              target="_blank"
              rel="noopener noreferrer"
              className="bg-purple-900/20 border border-purple-800/30 rounded-lg p-3 flex items-center gap-2 text-purple-300/80 hover:text-purple-200 transition-colors font-medium"
            >
              <Github className="h-4 w-4" />
              <span>nathank00/MLB-Analytics</span>
            </a>
          </div>

          <div
            onClick={() => router.push("/")}
            className="mt-auto text-center text-gray-200 text-sm cursor-pointer hover:text-purple-300"
          >
            © 1 OF 1 INTELLIGENCE LLC
          </div>
        </aside>
      )}

      {!sidebarVisible && (
        <button
          onClick={() => setSidebarVisible(true)}
          className="fixed top-4 left-4 z-30 text-white hover:text-purple-300 transition-colors"
          aria-label="Open sidebar"
        >
          <div className="mt-8 text-center text-white hover:text-purple-300 text-2xl font-light">[ ]</div>
        </button>
      )}

      <main className={`omega-page flex-1 p-6 transition-all duration-300 ease-in-out ${sidebarVisible ? "ml-[26rem]" : "ml-0"}`}>

        <div className="mt-4 relative text-center mb-8 inline-block w-full">
          <div
            className="inline-flex items-center gap-2 cursor-pointer bg-purple-900/30 px-4 py-2 rounded-lg border border-purple-800/50 hover:bg-purple-900/50"
            onClick={() => setShowDatePicker(!showDatePicker)}
          >
            <Calendar className="h-5 w-5 text-purple-400" />
            <h2 className="text-xl font-light text-purple-300">{formattedSelectedDate}</h2>
          </div>
          {showDatePicker && (
            <div
              ref={datePickerRef}
              className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 bg-purple-950 border border-purple-800 rounded-lg p-4 shadow-lg z-10"
            >
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => {
                  setSelectedDate(e.target.value)
                  setShowDatePicker(false)
                }}
                className="bg-purple-900/50 border border-purple-800 rounded px-3 py-2 text-purple-100 focus:outline-none focus:ring-2 focus:ring-purple-600"
              />
            </div>
          )}
          <div className="mt-5 mb-8 text-purple-300 text-m">
            <span className="text-green-400">{dailyRecord.wins} W</span>
            <span className="text-gray-500 mx-2">|</span>
            <span className="text-red-400">{dailyRecord.losses} L</span>
          </div>
        </div>

        {isLoading ? (
          <div className="text-center text-purple-400">Loading...</div>
        ) : error ? (
          <div className="text-center text-red-400">{error}</div>
        ) : games.length === 0 ? (
          <div className="text-center text-purple-400">No games found for this date.</div>
        ) : (
          <div className="grid gap-6 grid-cols-[repeat(auto-fit,minmax(320px,1fr))]">

            {games.map((game) => (
              <div
                key={game.game_id}
                className="bg-[#080110] border border-purple-900/50 rounded-lg p-4 shadow hover:shadow-purple-800/20"
              >
                <div className="flex justify-between items-center">
                <div className="flex items-center gap-4">
                {/* Away Team */}
                <div className="flex flex-col items-center">
                  <TeamLogo teamName={game.away_name} className="h-10 w-10" />
                  {game.game_started && game.runs_away !== undefined && (
                    <div className="text-purple-300 text-sm mt-1">{game.runs_away}</div>
                  )}
                </div>

                {/* @ symbol */}
                <div className="self-center text-purple-100 text-lg leading-none">@</div>

                {/* Home Team */}
                <div className="flex flex-col items-center">
                  <TeamLogo teamName={game.home_name} className="h-10 w-10" />
                  {game.game_started && game.runs_home !== undefined && (
                    <div className="text-purple-300 text-sm mt-1">{game.runs_home}</div>
                  )}
                </div>
              </div>

                {game.description && (
                  <div
                    className={`
                      font-normal text-sm flex items-center text-purple-300
                      ${game.game_started ? "bg-purple-800/60 px-2 py-1 rounded-full" : ""}
                    `}
                  >
                    {!game.game_started ? (
                      <>
                        <span>{game.description}</span>
                        <span className="mx-1 align-middle">·</span>
                        <Clock className="h-4 w-4 mr-1" />
                        {formatTime(game.start_time)}
                      </>
                    ) : (
                      <span>{game.description}</span>
                    )}
                  </div>
                )}


                </div>

                <div className="grid grid-cols-2 gap-3 mt-4">
                  <div className="bg-purple-900/30 p-3 rounded text-center border border-purple-900/30">
                    <div className="text-sm text-purple-400 mb-1">Runline</div>
                    <div className="text-xl">{game.runline ?? "N/A"}</div>
                  </div>
                  <div className="bg-purple-900/30 p-3 rounded text-center border border-purple-900/30 flex flex-col items-center justify-center">
                    <div className="text-sm text-purple-400 mb-2">Prediction</div>
                    <div className="text-xl flex items-center justify-center gap-2">
                      {renderPrediction(game.pick)}
                      {game.prediction_confidence !== undefined && game.prediction_confidence !== null ? (
                        <span
                          className={`text-sm ${
                            game.prediction_confidence >= 0.62 ? "text-green-400" : "text-white"
                          }`}
                        >
                          {(game.prediction_confidence * 100).toFixed(1)}%
                        </span>
                      ) : null}
                    </div>
                  </div>
                  <div className="bg-purple-900/30 p-3 rounded text-center border border-purple-900/30">
                    <div className="text-sm text-purple-400 mb-1">Total Runs</div>
                    <div className="text-xl">{game.runs_total ?? "-"}</div>
                  </div>
                  {(() => {
                    const outcome = getOutcomeStatus(game)

                    const outcomeTextColor =
                      outcome === "W" ? "text-green-400" :
                      outcome === "L" ? "text-red-400" :
                      outcome === "PUSH" ? "text-gray-400" :
                      "text-purple-300"

                    const outcomeBgColor =
                      outcome === "W" ? "bg-green-900/40" :
                      outcome === "L" ? "bg-red-900/40" :
                      outcome === "PUSH" ? "bg-gray-900/40" :
                      "bg-purple-900/30"

                    return (
                      <div className={`p-3 rounded text-center border border-purple-900/30 ${outcomeBgColor}`}>
                        <div className="text-sm text-purple-400 mb-3"></div>
                        <div className={`text-2xl ${outcomeTextColor}`}>{outcome}</div>
                      </div>
                    )
                  })()}


                  
                </div>

                <Dialog>
                  <DialogTrigger asChild>
                    <Button className="mt-4 w-full bg-purple-900/40 text-purple-300 border-purple-800 hover:bg-purple-800/70">
                      <Info className="h-4 w-4 mr-2" />
                      View Lineups
                    </Button>
                  </DialogTrigger>
                  <DialogContent className="bg-[#080110] border border-purple-900 text-purple-50 max-w-3xl">
                    <DialogHeader>
                      <DialogTitle className="text-lg font-light text-center text-purple-300 flex justify-center items-center gap-2">
                        <TeamLogo teamName={game.away_name} className="h-10 w-10" />
                        <span>@</span>
                        <TeamLogo teamName={game.home_name} className="h-10 w-10" />
                      </DialogTitle>
                    </DialogHeader>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-6">
                      <div>
                        <h3 className="text-purple-400 mb-2">{game.away_name} Lineup</h3>
                        <div className="text-purple-300 mb-2">SP: {game.Away_SP_Name}</div>
                        {[...Array(9)].map((_, i) => (
                          <div key={i} className="text-purple-200 font-light text-sm">
                            {i + 1}. {game[`Away_Batter${i + 1}_Name`] ?? "TBD"}
                          </div>
                        ))}
                      </div>
                      <div>
                        <h3 className="text-purple-400 mb-2">{game.home_name} Lineup</h3>
                        <div className="text-purple-300 mb-2">SP: {game.Home_SP_Name}</div>
                        {[...Array(9)].map((_, i) => (
                          <div key={i} className="text-purple-200 font-light text-sm">
                            {i + 1}. {game[`Home_Batter${i + 1}_Name`] ?? "TBD"}
                          </div>
                        ))}
                      </div>
                    </div>
                  </DialogContent>
                </Dialog>
              </div>
            ))}
          </div>
        )}
      </main>
    </main>
  )
}
