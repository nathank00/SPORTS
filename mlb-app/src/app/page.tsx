// app/page.tsx
"use client";

import { useRouter } from "next/navigation";
import { useRef, useState } from "react";

export default function Home() {
  const router = useRouter();
  const audioRoulette = useRef<HTMLAudioElement | null>(null);
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [hasTriggeredMedia, setHasTriggeredMedia] = useState(false);

  const handleTitleClick = () => {
    const audio = audioRoulette.current;
    const video = videoRef.current;
    if (!audio || !video) return;

    if (!hasTriggeredMedia) {
      setHasTriggeredMedia(true);
      video.play().catch((err) => console.error("Video play error:", err));
      audio.play().catch((err) => console.error("Audio play error:", err));
      setIsPlaying(true);
    } else {
      if (!isPlaying) {
        audio.currentTime = 0;
        audio.play();
        setIsPlaying(true);
      } else {
        audio.pause();
        setIsPlaying(false);
      }
    }
  };

  return (
    <div className="relative min-h-screen flex flex-col items-center pt-32 bg-black text-white overflow-hidden">
      {/* Audio */}
      <audio ref={audioRoulette} src="/music/ye.mp3" preload="auto" />

      {/* Video Background */}
      <video
        ref={videoRef}
        src="/video/vegas1.mp4"
        className={`absolute top-0 left-0 w-full h-full object-cover z-0 transition-opacity duration-500 ${
          hasTriggeredMedia ? "opacity-100" : "opacity-0"
        }`}
        muted
        loop
        playsInline
      />

      {/* Page Content */}
      <div className="relative z-10 w-full flex flex-col items-center">
        <div className="text-center mb-16">
          <h1
            className="text-5xl font-mono font-light tracking-wide mb-10 cursor-pointer hover:text-purple-400 transition-colors"
            onClick={handleTitleClick}
          >
            [ONE OF ONE INTELLIGENCE]
          </h1>
        </div>

        <div className="flex flex-col md:flex-row gap-16 mb-20">
          {/* ALPHA */}
          <div
            className="flex flex-col items-center cursor-pointer transition-transform hover:scale-105"
            onClick={() => router.push("/alpha")}
          >
            <div className="bg-teal-900/50 p-6 rounded-xl border border-teal-800/50 hover:border-teal-600/70 transition-colors">
              <img src="/team-logos/monkeyking5.png" alt="Alpha Model" className="w-48 h-auto" />
            </div>
            <span className="mt-4 text-2xl font-normal text-teal-300">[ ALPHA ]</span>
          </div>

          {/* OMEGA */}
          <div
            className="flex flex-col items-center cursor-pointer transition-transform hover:scale-105"
            onClick={() => router.push("/omega")}
          >
            <div className="bg-purple-900/50 p-6 rounded-xl border border-purple-800/50 hover:border-purple-600/70 transition-colors">
              <img src="/team-logos/monkeyking4.png" alt="Omega Model" className="w-48 h-auto" />
            </div>
            <span className="mt-4 text-2xl font-normal text-purple-300">[ OMEGA ]</span>
          </div>

          {/* SIGMA */}
          <div
            className="flex flex-col items-center cursor-pointer transition-transform hover:scale-105"
            onClick={() => router.push("/sigma")}
          >
            <div className="bg-orange-900/50 p-6 rounded-xl border border-orange-800/50 hover:border-orange-600/70 transition-colors">
              <img src="/team-logos/monkeyking.png" alt="Sigma Model" className="w-48 h-auto" />
            </div>
            <span className="mt-4 text-2xl font-normal text-orange-300">[ SIGMA ]</span>
          </div>
        </div>

        <div className="mt-auto mb-8 text-center text-gray-400 text-sm">
          <p>© 1 OF 1 INTELLIGENCE LLC</p>
        </div>
      </div>
    </div>
  );
}
