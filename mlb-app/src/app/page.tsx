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
    const [selectedDate, setSelectedDate] = useState(new Date().toISOString().split("T")[0]);

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

    useEffect(() => {
        fetchPicks(selectedDate);
    }, [selectedDate]);

    return (
        <div style={styles.container}>
            <h1 style={styles.title}>MLB Picks</h1>
            
            {/* Date Picker */}
            <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                style={styles.datePicker}
            />

            {error ? (
                <p style={styles.error}>{error}</p>
            ) : games.length === 0 ? (
                <p style={styles.noPicks}>No picks available.</p>
            ) : (
                <div style={styles.grid}>
                    {games.map((game, index) => (
                        <div key={index} style={styles.card}>
                            <h3 style={styles.teams}>
                                <span style={styles.teamName}>{game.home_team}</span> 
                                <span style={styles.vs}> vs </span> 
                                <span style={styles.teamName}>{game.away_team}</span>
                            </h3>
                            <p style={styles.text}>Runline: <span style={styles.highlight}>{game.runline}</span></p>
                            <p style={styles.text}>
                                Pick: 
                                <span style={{ 
                                    ...styles.pick, 
                                    backgroundColor: game.pick === "Over" ? "#4CAF50" : "#FF5252" 
                                }}>
                                    {game.pick}
                                </span>
                            </p>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

// Styling
const styles: { [key: string]: React.CSSProperties } = {
    container: {
        backgroundColor: "#121212",  // Dark background
        color: "#1E90FF",  // Blue accents
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        justifyContent: "center",
        padding: "20px",
    },
    title: {
        fontSize: "2.5rem",
        fontWeight: "bold",
        marginBottom: "20px",
        textTransform: "uppercase",
        letterSpacing: "2px",
    },
    datePicker: {
        backgroundColor: "#1E1E1E",
        border: "1px solid #1E90FF",
        padding: "10px",
        fontSize: "16px",
        borderRadius: "5px",
        color: "#1E90FF",
        outline: "none",
        marginBottom: "20px",
    },
    error: {
        color: "#FF5252",
        fontSize: "18px",
        marginTop: "10px",
    },
    noPicks: {
        fontSize: "18px",
        opacity: "0.8",
    },
    grid: {
        display: "grid",
        gridTemplateColumns: "repeat(auto-fit, minmax(300px, 1fr))",
        gap: "20px",
        width: "80%",
        maxWidth: "900px",
    },
    card: {
        backgroundColor: "#1E1E1E",
        padding: "20px",
        borderRadius: "10px",
        boxShadow: "0 4px 10px rgba(0, 0, 0, 0.3)",
        textAlign: "center",
        border: "2px solid #1E90FF",
        transition: "0.3s",
    },
    teams: {
        fontSize: "1.5rem",
        fontWeight: "bold",
    },
    teamName: {
        color: "#1E90FF",
        fontWeight: "bold",
    },
    vs: {
        color: "#1E90FF",
        fontSize: "1.3rem",
    },
    text: {
        fontSize: "1.2rem",
        marginTop: "10px",
    },
    highlight: {
        color: "#1E90FF",
        fontWeight: "bold",
    },
    pick: {
        fontSize: "1.3rem",
        fontWeight: "bold",
        color: "#121212",
        padding: "5px 10px",
        borderRadius: "5px",
        display: "inline-block",
    },
};
