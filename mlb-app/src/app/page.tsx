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

    useEffect(() => {
        fetch("/api/picks")
            .then((res) => res.json())
            .then((data) => {
                if (data.error) {
                    setError(data.error);
                } else {
                    setGames(data);
                }
            })
            .catch((err) => {
                console.error("Error fetching picks:", err);
                setError("Failed to load picks.");
            });
    }, []);

    return (
        <div>
            <h1>MLB Picks</h1>
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
