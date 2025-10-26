// app/sigma/page.tsx
"use client";

import { useEffect, useState, useRef } from "react";
import { format, parse } from "date-fns";
import { formatInTimeZone } from "date-fns-tz"; // Add date-fns-tz
import { Calendar, Info, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { useRouter } from "next/navigation";

type Prediction = {
  GAME_ID: string;
  AWAY_NAME: string;
  HOME_NAME: string;
  PREDICTION: number;
  TIMESTAMP: string;
};

export default function SigmaPage() {
  const router = useRouter();
  const datePickerRef = useRef<HTMLDivElement>(null);
  const [predictions, setPredictions] = useState<Prediction[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [lastUpdated, setLastUpdated] = useState("");
  const [selectedDate, setSelectedDate] = useState(() => {
    // Initialize with today's date in PDT
    return formatInTimeZone(new Date(), "America/Los_Angeles", "yyyy-MM-dd");
  });
  const [sidebarVisible, setSidebarVisible] = useState(true);
  const [showDatePicker, setShowDatePicker] = useState(false);

  const fetchPredictions = async () => {
    try {
      setLoading(true);
      setError("");
      const res = await fetch(`/api/predictions?date=${selectedDate}&_=${Date.now()}`);
      if (!res.ok) throw new Error(`Failed to fetch predictions: ${res.status}`);
      const data = await res.json();
      if (data.error) throw new Error(data.error);
      setPredictions(data);
      setLastUpdated(formatInTimeZone(new Date(), "America/Los_Angeles", "MMM d, h:mm a"));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load predictions");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchPredictions();
  }, [selectedDate]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (datePickerRef.current && !datePickerRef.current.contains(event.target as Node)) {
        setShowDatePicker(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const formatDate = (dateString: string): string => {
    const date = parse(dateString, "yyyy-MM-dd", new Date());
    return formatInTimeZone(date, "America/Los_Angeles", "eeee, MMMM d, yyyy");
  };

  // Placeholder daily record
  const dailyRecord = { wins: 0, losses: 0 };

  if (loading) {
    return (
      <div className="flex justify-center items-center h-screen bg-gradient-to-b from-gray-900 to-black text-orange-400">
        <div className="animate-spin rounded-full h-12 w-12 border-t-2 border-b-2 border-orange-400"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex justify-center items-center h-screen bg-gradient-to-b from-gray-900 to-black text-red-400">
        <p>{error}</p>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen bg-gradient-to-b from-gray-900 to-black text-white">
      {/* Sidebar */}
      <aside
        className={`w-64 bg-gray-900 border-r border-orange-800/30 p-4 flex flex-col fixed h-full transition-all duration-300 ease-in-out ${
          sidebarVisible ? "left-0" : "-left-64"
        }`}
      >
        <div className="mb-8"></div>
        <div className="mb-8 flex justify-center w-full">
          <h1
            className="text-2xl font-mono text-white mb-2 font-light tracking-wide cursor-pointer hover:text-orange-300 transition-colors whitespace-nowrap text-center"
            onClick={() => setSidebarVisible(false)}
          >
            [SIGMA]
          </h1>
        </div>
        <div className="mb-2 flex justify-center">
          <img src="/team-logos/monkeyking.png" alt="Monkey King" className="w-24 h-auto" />
        </div>
        <div className="mb-14">
          <div className="flex items-center justify-center gap-3 mb-2">
            <span className="text-green-400 font-light">{dailyRecord.wins} W</span>
            <span className="text-gray-500 mx-1">|</span>
            <span className="text-red-400 font-light">{dailyRecord.losses} L</span>
          </div>
          <div className="text-center text-orange-300/80 font-light text-sm">0.0% (0 games)</div>
        </div>
        <div className="space-y-2">
          <div className="bg-orange-900/20 border border-orange-800/30 rounded-lg p-3 flex items-center gap-2 text-orange-300/80 font-medium">
            <RefreshCw className="h-4 w-4" />
            <span>{lastUpdated || "Unavailable"}</span>
          </div>
        </div>
        <div
          className="mt-auto text-center text-gray-200 text-sm font-light cursor-pointer hover:text-orange-300 transition-colors"
          onClick={() => router.push("/")}
        >
          © 1 OF 1 INTELLIGENCE LLC
        </div>
      </aside>

      {/* Toggle Button */}
      {!sidebarVisible && (
        <button
          onClick={() => setSidebarVisible(true)}
          className="fixed top-4 left-4 z-20 text-white hover:text-orange-300 transition-colors"
          aria-label="Open sidebar"
        >
          <div className="mt-8 text-center text-white hover:text-orange-300 text-2xl font-light">[ ]</div>
        </button>
      )}

      {/* Main Content */}
      <main
        className={`flex-1 p-4 transition-all duration-300 ease-in-out ${
          sidebarVisible ? "ml-64" : "ml-0"
        }`}
      >
        <div className="max-w-6xl mx-auto">
          <div className="mb-8 text-center relative">
            <div className="flex flex-col items-center">
              <div
                className="inline-flex items-center mt-4 gap-2 cursor-pointer bg-orange-900/30 px-4 py-2 rounded-lg border border-orange-800/50 hover:bg-orange-900/50 transition-colors"
                onClick={() => setShowDatePicker(!showDatePicker)}
              >
                <Calendar className="h-5 w-5 text-orange-400" />
                <h2 className="text-xl font-light text-orange-300">{formatDate(selectedDate)}</h2>
              </div>
              {showDatePicker && (
                <div
                  ref={datePickerRef}
                  className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 bg-orange-950 border border-orange-800 rounded-lg p-4 shadow-lg z-10"
                >
                  <input
                    type="date"
                    value={selectedDate}
                    onChange={(e) => {
                      setSelectedDate(e.target.value);
                      setShowDatePicker(false);
                    }}
                    className="bg-orange-900/50 border border-orange-800 rounded px-3 py-2 text-orange-100 focus:outline-none focus:ring-2 focus:ring-orange-600"
                  />
                </div>
              )}
              <div className="mt-5 mb-8 text-orange-300 text-m">
                <span className="text-green-400 font-light">{dailyRecord.wins} W</span>
                <span className="text-gray-500 mx-2">|</span>
                <span className="text-red-400 font-light">{dailyRecord.losses} L</span>
              </div>
            </div>
          </div>

          {predictions.length === 0 ? (
            <div className="text-center p-12 text-gray-400">
              <p className="text-xl">No NBA predictions for {formatDate(selectedDate)}</p>
              <p className="text-sm mt-2">Select another date or try refreshing</p>
            </div>
          ) : (
            <div className="grid gap-6 md:grid-cols-2 lg:grid-cols-3">
              {predictions.map((p) => (
                <div
                  key={p.GAME_ID}
                  className="bg-gray-900/50 border border-orange-800/30 rounded-xl p-6 hover:border-orange-600/50 transition-colors"
                >
                  <div className="flex justify-between items-start mb-4">
                    <div className="flex-1">
                      <p className="text-sm text-gray-400">Away</p>
                      <p className="font-semibold">{p.AWAY_NAME}</p>
                    </div>
                    <div className="text-center mx-4">
                      <p className="text-2xl font-bold text-orange-400">@</p>
                    </div>
                    <div className="flex-1 text-right">
                      <p className="text-sm text-gray-400">Home</p>
                      <p className="font-semibold">{p.HOME_NAME}</p>
                    </div>
                  </div>

                  <div className="text-center mb-4">
                    <p className="text-sm text-gray-400">Predicted winner</p>
                    <p
                      className={`text-xl font-semibold ${
                        p.PREDICTION === 1 ? "text-green-400" : "text-red-400"
                      }`}
                    >
                      {p.PREDICTION === 1 ? p.HOME_NAME : p.AWAY_NAME}
                    </p>
                  </div>

                  <Dialog>
                    <DialogTrigger asChild>
                      <Button className="w-full mt-2 bg-orange-900/40 text-orange-300 border-orange-800 hover:bg-orange-800/70">
                        <Info className="h-4 w-4 mr-2" />
                        Details
                      </Button>
                    </DialogTrigger>
                    <DialogContent className="bg-gray-900 border-orange-800 text-orange-50">
                      <DialogHeader>
                        <DialogTitle className="text-orange-400">Game Details</DialogTitle>
                      </DialogHeader>
                      <div className="space-y-2 text-sm">
                        <p>
                          <span className="text-gray-400">Game ID:</span> {p.GAME_ID}
                        </p>
                        <p>
                          <span className="text-gray-400">Predicted:</span>{" "}
                          {p.PREDICTION === 1 ? `${p.HOME_NAME} Win` : `${p.AWAY_NAME} Win`}
                        </p>
                        <p>
                          <span className="text-gray-400">Timestamp:</span> {p.TIMESTAMP}
                        </p>
                      </div>
                    </DialogContent>
                  </Dialog>
                </div>
              ))}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
