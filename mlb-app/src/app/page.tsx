"use client";
import { useEffect, useState } from "react";

type GamePick = {
    game_id: number;
    home_team: string;
    away_team: string;
    runline: string;
    pick: string;
};

export default function Home() {
    const [games, setGames] = useState<GamePick[]>([]);
    const [error, setError] = useState("");
    const [selectedDate, setSelectedDate] = useState(new Date().toISOString().split("T")[0]); // Default to today

    const fetchPicks = (date: string) => {
        fetch(`/api/picks?date=${date}`)
            .then((res) => res.json())
            .then((data) => {
                if (data.error) {
                    setError(data.error);
                    setGames([]);
                } else {
                    setError("");
                    setGames(data);
                }
            })
            .catch((err) => {
                console.error("Error fetching picks:", err);
                setError("Failed to load picks.");
                setGames([]);
            });
    };

    // Fetch picks whenever the date changes
    useEffect(() => {
        fetchPicks(selectedDate);
    }, [selectedDate]);

    return (
        <div>
            <h1>MLB Picks</h1>
            
            {/* Date Picker */}
            <label>Select Date: </label>
            <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
            />

            {error ? (
                <p>{error}</p>
            ) : games.length === 0 ? (
                <p>No picks available.</p>
            ) : (
                games.map((game, index) => (
                    <div key={index} style={{ border: "1px solid black", marginBottom: "10px", padding: "10px" }}>
                        <h3>{game.home_team} vs {game.away_team}</h3>
                        <p>Runline: {game.runline}</p>
                        <p>Pick: {game.pick}</p>
                    </div>
                ))
            )}
        </div>
    );
}
